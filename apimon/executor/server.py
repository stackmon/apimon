# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
import json
import logging
import threading
import traceback
import time
import socket
import os
import random
import shutil
import signal
import subprocess
import yaml
import configparser

from pathlib import Path

import gear

from apimon.project import Project
from apimon.lib import commandsocket
from apimon.lib.logutils import get_annotated_logger
from apimon.lib.gearworker import GearWorker

from apimon.executor.sensors.cpu import CPUSensor
from apimon.executor.sensors.pause import PauseSensor

from apimon.ansible import logconfig


COMMANDS = ['stop', 'pause', 'resume']


def preexec_function():
    """Pre-exec to hide SIGINT for child process
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class ExecutorExecuteWorker(gear.TextWorker):
    def __init__(self, executor_server, *args, **kw):
        self.executor_server = executor_server
        super(ExecutorExecuteWorker, self).__init__(*args, **kw)

    def handleNoop(self, packet):
        # Delay our response to running a new job based on the number
        # of jobs we're currently running, in an attempt to spread
        # load evenly among executors.
        workers = len(self.executor_server.job_workers)
        delay = (workers ** 2) / 1000.0
        time.sleep(delay)
        return super(ExecutorExecuteWorker, self).handleNoop(packet)


class AnsibleJob:

    def __init__(self, executor_server, job):
        self.executor_server = executor_server
        self.job = job
        self.arguments = json.loads(job.arguments)
        self.job_id = self.arguments.get('job_id')

        logger = logging.getLogger("apimon.AnsibleJob")
        self.log = get_annotated_logger(
            logger, job.unique, self.job_id)

        self.job_work_dir = Path(
            self.executor_server.config.get_default(
                'executor', 'work_dir'), job.unique)

        self.ansible_plugin_path = \
            Path(
                Path(__file__).resolve().parent.parent,
                'ansible', 'callback').as_posix()

    def run(self):
        self.running = True
        self.thread = threading.Thread(target=self.execute,
                                       name='job-%s' % self.job.unique)
        self.thread.start()

    def stop(self):
        pass

    def wait(self):
        if self.thread:
            self.thread.join()

    def _prepare_local_work_dir(self):
        project_args = self.arguments.get('project')
        project = self.executor_server._projects[project_args['name']]
        # Copy project checkout to local wrk_dir
        shutil.copytree(project.project_dir, self.job_work_dir)
        self.local_project = Project(
            project.name,
            project.repo_url,
            exec_cmd=project.exec_cmd)

    def execute(self):
        try:
            self._prepare_local_work_dir()

            self._execute()
        except Exception:
            self.log.exception("Exception while executing job")
            self.job.sendWorkException(traceback.format_exc())
        finally:
            self.running = False
            # shutil.rmtree(self.job_work_dir)
            try:
                self.executor_server.finish_job(self.job.unique)
            except Exception:
                self.log.exception("Error finalizing job thread:")

    def _prepare_ansible_cfg(self, work_dir):
        config = configparser.ConfigParser()

        ansible_cfg = Path(work_dir, 'ansible.cfg')

        if ansible_cfg.exists():
            # The ansible cfg already exists - read it
            config.read(ansible_cfg.as_posix())
        else:
            config['defaults'] = {}

        config['defaults']['stdout_callback'] = 'apimon_logger'

        with open(ansible_cfg, 'w') as f:
            config.write(f)

    def _execute(self):
        # report that job has been taken
        self.job.sendWorkData(json.dumps(self._base_job_data()))

        self.job.sendWorkStatus(0, 100)

        job_log_dir = Path(self.job_work_dir)
        job_log_dir.mkdir(parents=True, exist_ok=True)
        # Generate job log config
        job_log_file = Path(job_log_dir, 'job-output.txt')
        job_log_config = logconfig.JobLoggingConfig(
            job_output_file=job_log_file.as_posix())
        job_log_config_file = Path(job_log_dir, 'logging.json').as_posix()
        job_log_config.writeJson(job_log_config_file)
        self._prepare_ansible_cfg(job_log_dir)

        self.job.sendWorkStatus(1, 100)

        env = os.environ.copy()
        env['TASK_EXECUTOR_JOB_ID'] = self.job_id
        env['ANSIBLE_CALLBACK_PLUGINS'] = \
            self.ansible_plugin_path
        env['APIMON_EXECUTOR_JOB_CONFIG'] = job_log_config_file
        env.update(self.arguments.get('env').get('vars'))

        cmd = (self.local_project.get_exec_cmd(
            self.arguments['project']['task'])).split(' ')
        if not self.executor_server.config.get_default(
                'executor', 'dry_run', False):
            self.log.debug('Starting execution of %s' % cmd)

            process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       preexec_fn=preexec_function,
                                       env=env,
                                       cwd=self.job_work_dir,
                                       restore_signals=False)

            # Read the output
            for line in process.stdout:
                self.log.debug('%s', line.decode('utf-8'))
            stderr = process.stderr.read()
            if stderr:
                self.log.error('%s', stderr.decode('utf-8'))

            # Wait for child process to finish
            process.wait()

        else:
            self.log.debug('Simulating execution of %s in %s' % (cmd, env))

        self.job.sendWorkComplete(json.dumps({'a': 'complete'}))

    def _base_job_data(self):
        return {
            # TODO(mordred) worker_name is needed as a unique name for the
            # client to use for cancelling jobs on an executor. It's
            # defaulting to the hostname for now, but in the future we
            # should allow setting a per-executor override so that one can
            # run more than one executor on a host.
            'worker_name': self.executor_server.name,
            'worker_hostname': self.executor_server.hostname,
        }


class ExecutorServer:

    log = logging.getLogger('apimon.ExecutorServer')

    def __init__(self, config):
        self.config = config
        self._running = False

        self.hostname = self.config.get_default('executor', 'hostname',
                                                socket.getfqdn())
        self.name = '%s:%s' % (self.hostname, os.getpid())

        self.governor_lock = threading.Lock()
        self.run_lock = threading.Lock()

        self.command_map = dict(
            stop=self.stop,
            pause=self.pause,
            resume=self.resume,
        )
        command_socket = self.config.get_default(
            'executor', 'socket',
            '/var/lib/apimon/executor.socket')
        self.command_socket = commandsocket.CommandSocket(command_socket)

        self._command_running = False
        self.accepting_work = False

        cpu_sensor = CPUSensor(config)
        self.pause_sensor = PauseSensor(False)
        self.sensors = [
            cpu_sensor,
            self.pause_sensor
        ]

        self.job_workers = {}
        self._projects = {}
        self._config_version = None
        self._clouds_config = {}

        self.executor_jobs = {
            'apimon:ansible': self.execute_ansible_job
        }

        self.executor_worker = GearWorker(
            'APImon Executor',
            'apimon.ExecutorWorker',
            'executor',
            self.config,
            self.executor_jobs,
            worker_class=ExecutorExecuteWorker,
            worker_args=[self]
        )

        self.gear_client = gear.Client()
        for server in self.config.get_section('gear'):
            self.gear_client.addServer(
                server.get('host'), server.get('port'),
                server.get('ssl_key'), server.get('ssl_cert'),
                server.get('ssl_ca'),
                keepalive=True, tcp_keepidle=60,
                tcp_keepintvl=30, tcp_keepcnt=5)
        self.gear_client.waitForServer()

    def start(self) -> None:
        self._running = True
        self._command_running = True
        self.log.debug("Starting command processor")

#        try:
#            multiprocessing.set_start_method('spawn')
#        except RuntimeError:
#            # Note: During tests this can be called multiple times which
#            # results in a runtime error. This is ok here as we've set this
#            # already correctly.
#            self.log.warning('Multiprocessing context has already been set')

        self.command_socket.start()
        self.command_thread = threading.Thread(
            target=self.run_command, name='command')
        self.command_thread.daemon = True
        self.command_thread.start()

        self.executor_worker.start()

        self.governor_stop_event = threading.Event()
        self.governor_thread = threading.Thread(target=self.run_governor,
                                                name='governor')
        self.governor_thread.daemon = True
        self.governor_thread.start()

    def stop(self) -> None:
        self.log.debug("Stopping")
        # The governor can change function registration, so make sure
        # it has stopped.
        self.governor_stop_event.set()
        self.governor_thread.join()

        # Tell the executor worker to abort any jobs it just accepted,
        # and grab the list of currently running job workers.
        with self.run_lock:
            self._running = False
            self._command_running = False
            workers = list(self.job_workers.values())

        # Stop asking for new jobs
        self.executor_worker.pause()

        for job_worker in workers:
            try:
                job_worker.stop()
            except Exception:
                self.log.exception("Exception sending stop command "
                                   "to worker:")
        self.wait_for_jobs(workers)
        self.executor_worker.gearman.setFunctions([])

        self.executor_worker.stop()

        self.command_socket.stop()

        self.log.debug("Stopped")

    def join(self) -> None:
        self.governor_thread.join()
        self.executor_worker.join()

    def pause(self) -> None:
        """Pause processing"""
        self.log.debug('Pausing')
        self.pause_sensor.pause = True
        self.executor_worker.pause()

    def resume(self) -> None:
        """Resume processing of jobs"""
        self.log.debug('Resuming')
        self.pause_sensor.pause = False
        self.executor_worker.resume()

    def run_command(self) -> None:
        """Command loop"""
        while self._command_running:
            try:
                command = self.command_socket.get().decode('utf8')
                if command != '_stop':
                    self.command_map[command]()
            except Exception:
                self.log.exception("Exception while processing command")

    def run_governor(self) -> None:
        """Governor (load controller) loop"""
        while not self.governor_stop_event.wait(10):
            try:
                self.manage_load()
            except Exception:
                self.log.exception("Exception in governor thread:")

    def _flush_clouds_config(self) -> None:
        """Write clouds.yaml to FS into home dir"""
        clouds_loc = Path(Path.home(), '.config', 'openstack')
        clouds_loc.mkdir(parents=True, exist_ok=True)
        with open(Path(clouds_loc, 'clouds.yaml'), 'w') as f:
            yaml.dump(self._clouds_config, f, default_flow_style=False)

    def execute_ansible_job(self, job) -> None:
        """Main entry function for ansible job requests"""
        args = json.loads(job.arguments)
        job_id = args.get('job_id')

        log = get_annotated_logger(self.log, job.unique, job_id)

        log.debug('Got %s job: %s', job.name, job.unique)

        config_version = args.get('config_version')
        if config_version != self._config_version:
            log.debug('Requesting clouds config')
            self._get_clouds_config(config_version)
            self._flush_clouds_config()

        project_args = args.get('project')

        if not project_args:
            raise RuntimeError('Job not supported')

        project = self._projects.get(project_args.get('name'))

        if not project:
            project = Project(
                name=project_args.get('name'),
                repo_url=project_args.get('url'),
                repo_ref=project_args.get('ref'),
                exec_cmd=project_args.get('exec_cmd'),
                work_dir=self.config.get_default(
                    'executor', 'work_dir'),
                commit=project_args.get('commit')
            )
            project.get_git_repo()
            project.prepare()
            self._projects[project.name] = project
        if str(project.get_commit()) != \
                project_args.get('commit'):
            self.log.debug('current commit is %s' % project.get_commit())
            project.refresh_git_repo()
            project.prepare()

        self.job_workers[job.unique] = AnsibleJob(self, job)
        self.manage_load()
        self.job_workers[job.unique].run()
        # NOTE(gtema): sleep a bit not to accept all jobs immediately on start
        time.sleep(1)

    def manage_load(self) -> None:
        with self.governor_lock:
            return self._manage_load()

    def _manage_load(self) -> None:
        if self.accepting_work:
            # Don't unregister if we don't have any active jobs.
            for sensor in self.sensors:
                ok, message = sensor.isOk()
                if not ok:
                    self.log.info(
                        "Unregistering due to {}".format(message))
                    self.unregister_work()
                    break
        else:
            reregister = True
            limits = []
            for sensor in self.sensors:
                ok, message = sensor.isOk()
                limits.append(message)
                if not ok:
                    reregister = False
                    break
            if reregister:
                self.log.info("Re-registering as job is within its limits "
                              "{}".format(", ".join(limits)))
                self.register_work()

    def finish_job(self, unique) -> None:
        """A callback after the job finished processing"""
        del(self.job_workers[unique])

    def register_work(self) -> None:
        if self._running:
            self.accepting_work = True
            function_name = 'apimon:ansible'
            self.executor_worker.gearman.registerFunction(function_name)
            self.executor_worker.resume()
            self.executor_worker.gearman._sendGrabJobUniq()

    def unregister_work(self) -> None:
        self.accepting_work = False
        function_name = 'apimon:ansible'
        self.executor_worker.pause()

        self.wait_for_jobs()

        self.executor_worker.gearman.unRegisterFunction(function_name)

    def wait_for_jobs(self, workers=None) -> None:
        self.log.debug('Waiting for current jobs to finish')
        if not workers:
            with self.run_lock:
                workers = list(self.job_workers.values())

        for job_worker in workers:
            try:
                job_worker.wait()
            except Exception:
                self.log.exception("Exception waiting for worker "
                                   "to stop:")

    def _get_clouds_config(self, version=None) -> None:
        """Get the config from scheduler"""
        get_config_job = gear.TextJob('apimon:get_cloud_config',
                                      json.dumps({'version': version}))
        self.gear_client.submitJob(get_config_job)
        while not get_config_job.complete:
            time.sleep(1)
            res = get_config_job.data
            if not len(res):
                raise RuntimeError("get_config_job didn't return data")
            d = res[-1]
            if d:
                data = json.loads(d)
                self._config_version = data.pop('_version')
                self._clouds_config = data