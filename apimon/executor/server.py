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
import re
import shutil
import signal
import subprocess
import yaml
import configparser

from pathlib import Path

import gear

import openstack

from apimon.lib.statsd import get_statsd

try:
    from alertaclient.api import Client as alerta_client
except ImportError:
    alerta_client = None

from apimon.lib import commandsocket
from apimon.lib.logutils import get_annotated_logger
from apimon.lib.gearworker import GearWorker
from apimon.project import Project

from apimon.executor import resultprocessor
from apimon.executor import message
from apimon.executor.sensors.cpu import CPUSensor
from apimon.executor.sensors.pause import PauseSensor

from apimon.ansible import logconfig


COMMANDS = ['stop', 'pause', 'resume']


RE_EXC = re.compile(r"(?P<exception>\w*\b):\s(?P<code>\d{3})\b")
RE_EXC2 = re.compile(r"\((?P<exception>\w+) (?P<code>\d{3})\)")
RE_500 = re.compile(r"Internal\s?Server\s?Error")


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


class BaseJob:
    """Base functionality of any project type"""

    def __init__(self, executor_server, job):
        self.executor_server = executor_server
        self.job = job
        self.running = False

        self.arguments = json.loads(job.arguments)
        self.job_id = self.arguments.get('job_id')

        logger = logging.getLogger("apimon.executor.BaseJob")
        self.log = get_annotated_logger(
            logger, job.unique, self.job_id)

        self.job_work_dir = Path(
            self.executor_server.config.get_default(
                'executor', 'work_dir'), job.unique)

        self.statsd_extra_keys = {
            'zone': self.executor_server.zone,
            'environment': self.arguments['env']['name']
        }
        try:
            self.statsd = get_statsd(
                self.executor_server.config, self.statsd_extra_keys)
        except socket.gaierror:
            pass

        self.socket_path = Path(
            self.job_work_dir, '.comm_socket').resolve().as_posix()
        self.socket = None
        self.socket_thread = None

        self.thread = None

    def run(self) -> None:
        """Main entry point"""
        self.running = True
        self.thread = threading.Thread(target=self.execute,
                                       name='job-%s' % self.job.unique)
        self.thread.start()

    def stop(self) -> None:
        pass

    def wait(self) -> None:
        if self.thread:
            self.thread.join()

    def _setup_communication_socket(self) -> None:
        """Setup socket for communication with the child process"""
        try:
            if os.path.exists(self.socket_path):
                os.unlink(self.socket_path)
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.socket.bind(self.socket_path)
            self.socket.listen(5)
            self.socket_thread = threading.Thread(target=self._socketListener)
            self.socket_thread.daemon = True
            self.socket_thread.start()
        except Exception:
            self.log.exception(
                "Error starting communication socket. Continuing")

    def _teardown_communication_socket(self) -> None:
        # Shut down communication socket
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                if self.socket:
                    s.connect(self.socket_path)
                    s.sendall(b'_stop\n')
        except Exception:
            self.log.exception("Error finalizing communitation socket")
        finally:
            if self.socket_thread:
                self.socket_thread.join()
            if self.socket:
                self.socket.close()
            if os.path.exists(self.socket_path):
                try:
                    os.unlink(self.socket_path)
                except Exception:
                    pass

    def _socketListener(self) -> None:
        """Socket listener (thread) function"""
        while self.running:
            try:
                s, addr = self.socket.accept()
                self._talk_through_socket(s)
            except Exception:
                self.log.exception("Exception in socket handler")

    def _talk_through_socket(self, conn) -> None:
        """Socket connection function"""
        try:
            buf = b''
            while True:
                buf += conn.recv(1)
                if buf[-1:] == b'\n':
                    break
            buf = buf.strip()
            self.log.debug("Received %s from socket" % (buf,))
            # Because we use '_stop' internally to wake up a
            # waiting thread, don't allow it to actually be
            # injected externally.
            if buf != b'_stop':
                data = None
                try:
                    data = message.get_message(buf)
                except json.JSONDecodeError:
                    self.log.exception("Exception decoding metric")
                if data and self.statsd_extra_keys:
                    data.update(self.statsd_extra_keys)
                if isinstance(data, message.Metric):
                    name_segments = [
                        'apimon', 'metric', '{environment}',
                        '{zone}', data['name']
                    ]
                    if 'az' in data:
                        name_segments.append(data['az'])
                    name = '.'.join(name_segments)
                    val = data.get('value', 1)
                    if data['metric_type'] == 'c':
                        self.statsd.incr(name, val)
                    elif data['metric_type'] == 'ms':
                        # Increment 'attempted' counter
                        self.statsd.incr('%s.attempted' % name, 1)
                        name_suffix = data.get('name_suffix')
                        if name_suffix:
                            # Expecting name_suffix to be something like
                            # 'passed'
                            name = '%s.%s' % (name, name_suffix)
                            # increment '__suffix__' counter
                            self.statsd.incr(name, 1)
                        # save the timing
                        self.statsd.timing(name, val)
                    elif data['metric_type'] == 'g':
                        self.statsd.gauge(name, val)
                elif isinstance(data, message.ResultTask):
                    data['job_id'] = self.job_id
                    self.executor_server.result_processor.add_entry(data)
                    if data['result'] == 3 and self.executor_server.alerta:
                        try:
                            alert = self._prepare_alert_data(data)
                            self.executor_server.alerta.send_alert(**alert)
                        except Exception:
                            self.log.exception('Exception raising alert')

                elif isinstance(data, message.ResultSummary):
                    data['job_id'] = self.job_id
                    self.executor_server.result_processor.add_entry(data)

                else:
                    self.log.error('Unsupported metric data %s' % buf)
        except Exception:
            self.log.exception("Exception in socket communication")
        finally:
            conn.close()

    def _get_message_error_category(self, msg: str = None) -> str:
        result = ''
        if not msg:
            return ''
        # SomeSDKException: 123
        # ResourceNotFound: 404
        exc = RE_EXC.search(msg)
        if not exc:
            # (HTTP 404)
            exc = RE_EXC2.search(msg)
        if exc:
            result = "%s%s" % (exc.group('exception'), exc.group('code'))
            return result

        # contains InternalServerError
        if RE_500.search(msg):
            result = "HTTP500"
            return result
        # Quota exceeded, quota exceeded, exceeded for quota
        if 'uota ' in msg and ' exceed' in msg:
            result = "QuotaExceeded"
            return result
        # WAF
        if 'The incident ID is:' in msg:
            result = "WAF"
            return result
        if 'ResourceTimeout' in msg or 'Read timed out' in msg:
            return 'ResourceTimeout'
        if 'Connection reset by peer' in msg:
            return 'ConnectionReset'
        if 'Could not detach attachment' in msg and 'scenario16' in msg:
            return 'HeatDetachVolume'

        return result if result else msg

    def _prepare_alert_data(self, data: dict) -> dict:
        link = self.executor_server._get_logs_link(self.job_id)
        web_link = '<a href="%s">%s</a>' % (link, self.job_id)

        _service = data.get('service')
        service = ['apimon']
        if _service:
            service.append(_service)
        alert_data = dict(
            origin=('executor@%s' % self.executor_server.zone),
            environment=data.get('environment'),
            service=service,
            resource=data.get('action'),
            event=self._get_message_error_category(
                data.get('anonymized_response', 'N/A')),
            severity='major',
            value=data.get('anonymized_response'),
            raw_data=data.get('raw_response'),
            type='ApimonExecutorAlert',
            attributes={
                'logUrl': link,
                'logUrlWeb': web_link,
                'state': data.get('state', 'unknown')
            }
        )

        self.log.debug('Alerta info %s' % alert_data)

        return alert_data

    def _prepare_local_work_dir(self) -> None:
        project_args = self.arguments.get('project')
        project = self.executor_server._projects[project_args['name']]
        # Copy project checkout to local wrk_dir
        shutil.copytree(project.project_dir, self.job_work_dir,
                        dirs_exist_ok=True)
        self.local_project = Project(
            project.name,
            project.repo_url,
            exec_cmd=project.exec_cmd)

    def execute(self) -> None:
        try:
            self._prepare_local_work_dir()
            self._setup_communication_socket()
            self.job.sendWorkData(json.dumps(self._base_job_data()))
            self.job.sendWorkStatus(1, 100)
            job_log_dir = Path(self.job_work_dir)
            job_log_dir.mkdir(parents=True, exist_ok=True)
            # Generate job log config
            job_log_file = Path(job_log_dir, 'job-output.txt')

            time_start = time.time_ns()
            self._execute(job_log_dir, job_log_file)
            time_end = time.time_ns()

            self.job.sendWorkComplete(json.dumps(
                {
                    'task': self.arguments['project']['task'],
                    'result': 'complete',
                    'duration_sec': int((time_end - time_start) / 1000000000),
                    'project': {
                        'name': self.arguments['project']['name'],
                        'task': self.arguments['project']['task'],
                    },
                    'env': {
                        'name': self.arguments['env']['name']
                    }
                }
            ))
            try:
                self.executor_server._upload_log_file_to_swift(
                    job_log_file, self.job_id)
            except Exception:
                self.log.exception('Exception saving job log')

            try:
                job_log_file.unlink()
            except Exception:
                pass

        except Exception:
            self.log.exception("Exception while executing job")
            self.job.sendWorkException(traceback.format_exc())
        finally:
            self.running = False
            self._teardown_communication_socket()
            try:
                shutil.rmtree(self.job_work_dir)
            except Exception:
                pass
            try:
                self.executor_server.finish_job(self.job.unique)
            except Exception:
                self.log.exception("Error finalizing job thread:")

    def _execute(self, job_log_dir: Path, job_log_file: Path) -> None:
        """Main execution logic function"""

        self.job.sendWorkStatus(2, 100)

        env = os.environ.copy()
        env['TASK_EXECUTOR_JOB_ID'] = self.job_id
        env.update(self.arguments.get('env').get('vars'))

        cmd = (self.local_project.get_exec_cmd(
            self.arguments['project']['task'])).split(' ')
        if not self.executor_server.config.get_default(
                'executor', 'dry_run', False):
            self.log.debug('Starting execution of %s' % cmd)
            with open(job_log_file, 'w') as log_fd:
                process = subprocess.Popen(
                    cmd,
                    stdout=log_fd, stderr=subprocess.STDOUT,
                    preexec_fn=preexec_function,
                    env=env,
                    cwd=self.job_work_dir,
                    restore_signals=False)

                # Wait for child process to finish
                process.wait()

            if self.log.isEnabledFor(logging.DEBUG):
                with open(job_log_file, 'r') as log_fd:
                    # Show the job output into our log
                    for line in log_fd:
                        self.log.debug('%s', line)

        else:
            self.log.debug('Simulating execution of %s in %s' % (cmd, env))

    def _base_job_data(self) -> dict:
        return {
            'worker_name': self.executor_server.name,
            'worker_hostname': self.executor_server.hostname,
        }


class AnsibleJob(BaseJob):
    """Base functionality of the ansible project"""

    def __init__(self, executor_server, job):
        super(AnsibleJob, self).__init__(executor_server, job)
        logger = logging.getLogger("apimon.executor.AnsibleJob")
        self.log = get_annotated_logger(
            logger, job.unique, self.job_id)

        self.ansible_plugin_path = \
            Path(
                Path(__file__).resolve().parent.parent,
                'ansible', 'callback').as_posix()

    def _prepare_ansible_cfg(self, work_dir):
        config = configparser.ConfigParser()

        ansible_cfg = Path(work_dir, 'ansible.cfg')

        if ansible_cfg.exists():
            # The ansible cfg already exists - read it
            config.read(ansible_cfg.as_posix())
        else:
            config['defaults'] = {}
            config['callback_apimon_profiler'] = {}

        config['defaults']['stdout_callback'] = 'apimon_logger'
        config['defaults']['callback_whitelist'] = 'apimon_profiler'
        config['callback_apimon_profiler']['socket'] = \
            self.socket_path

        with open(ansible_cfg, 'w') as f:
            config.write(f)

    def _execute(self, job_log_dir: Path, job_log_file: Path) -> None:
        """Execute the task"""

        job_log_config = logconfig.JobLoggingConfig(
            job_output_file=job_log_file.resolve().as_posix())
        job_log_config_file = Path(job_log_dir, 'logging.json').resolve()
        job_log_config.writeJson(job_log_config_file)
        self._prepare_ansible_cfg(job_log_dir)

        self.job.sendWorkStatus(1, 100)

        env = os.environ.copy()
        env['TASK_EXECUTOR_JOB_ID'] = self.job_id
        env['ANSIBLE_CALLBACK_PLUGINS'] = \
            self.ansible_plugin_path
        env['APIMON_EXECUTOR_JOB_CONFIG'] = job_log_config_file
        env['ANSIBLE_PYTHON_INTERPRETER'] = '/usr/bin/python3'
        env.update(self.arguments.get('env').get('vars'))
        # Inform statsd (ansible->openstacksdk->statsd) about our desired stats
        env['STATSD_PREFIX'] = (
            'openstack.api.{environment}.{zone}'
            .format(**self.statsd_extra_keys)
        )

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


class MiscJob(BaseJob):
    """Base functionality of the misc project"""

    def __init__(self, executor_server, job):
        super(MiscJob, self).__init__(executor_server, job)
        logger = logging.getLogger("apimon.executor.MiscJob")
        self.log = get_annotated_logger(
            logger, job.unique, self.job_id)


class ExecutorServer:

    log = logging.getLogger('apimon.ExecutorServer')

    def __init__(self, config, zone: str = None):
        self.log.info('Starting Executor server')
        self.config = config
        self._running = False
        self.zone = zone or config.get_default(
            'executor', 'zone', 'default_zone')

        self.hostname = self.config.get_default('executor', 'hostname',
                                                socket.getfqdn())
        self.name = '%s:%s' % (self.hostname, os.getpid())

        self.governor_lock = threading.RLock()
        self.run_lock = threading.Lock()

        self.command_map = dict(
            stop=self.stop,
            pause=self.pause,
            resume=self.resume
        )
        command_socket = self.config.get_default(
            'executor', 'socket',
            '/var/lib/apimon/executor.socket')
        self.command_socket = commandsocket.CommandSocket(command_socket)

        self.statsd_extra_keys = {
            'zone': self.zone,
            'hostname': self.hostname
        }
        self.statsd = get_statsd(self.config, self.statsd_extra_keys)

        self.result_processor = resultprocessor.ResultProcessor(self.config)

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
        self.alerta = None

        self.executor_jobs = {
            'apimon:ansible': self.execute_ansible_job,
            'apimon:misc': self.execute_misc_job
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

        self.log.debug('Connecting to gearman servers')
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

        self.command_socket.start()
        self.command_thread = threading.Thread(
            target=self.run_command, name='command')
        self.command_thread.daemon = True
        self.command_thread.start()
        self.result_processor.daemon = True
        self.result_processor.start()

        if alerta_client:
            alerta_ep = self.config.get_default('alerta', 'endpoint')
            alerta_token = self.config.get_default('alerta', 'token')
            if alerta_ep and alerta_token:
                self.alerta = alerta_client(
                    endpoint=alerta_ep,
                    key=alerta_token)

        self.accepting_work = True

        self.executor_worker.start()

        self.governor_stop_event = threading.Event()
        self.governor_thread = threading.Thread(target=self.run_governor,
                                                name='governor')
        self.governor_thread.daemon = True
        self.governor_thread.start()

    def stop(self) -> None:
        self.log.info("Stopping")
        # Stop asking for new jobs
        self.executor_worker.pause()

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
        self.result_processor.stop()

        if self.statsd:
            # Zero the executor load
            base_key = 'apimon.executor.{zone}.{hostname}'
            for sensor in self.sensors:
                sensor.reportStats(self.statsd, base_key, zero=True)

        self.log.info("Stopped")

    def join(self) -> None:
        self.governor_thread.join()
        self.executor_worker.join()

    def pause(self) -> None:
        """Pause processing"""
        self.log.debug('Pausing')
        self.pause_sensor.pause = True
        self.accepting_work = False
        self.executor_worker.pause()

    def resume(self) -> None:
        """Resume processing of jobs"""
        self.log.debug('Resuming')
        self.pause_sensor.pause = False
        self.accepting_work = True
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
            self._send_alerta_heartbeat()

    def _send_alerta_heartbeat(self):
        if self.alerta:
            try:
                self.alerta.heartbeat(
                    origin='apimon.executor.%s' % self.hostname,
                    tags=['apimon', 'executor'],
                    timeout=300
                )
            except Exception:
                self.log.exception('Error sending heartbeat')

    def _create_logs_container(self, connection, container_name):
        container = connection.object_store.create_container(
            name=container_name)
        container.set_metadata(
            connection.object_store,
            metadata={
                'read_ACL': '.r:*,.rlistings',
                'web_index': 'index.html',
                'web_listings': 'True'
            }
        )
        return container

    def _get_logs_link(self, job_id: str):
        if self._logs_cloud:
            return '{ep}/{container}/{job_id}'.format(
                ep=self._logs_cloud.object_store.get_endpoint(),
                container=self._logs_container_name,
                job_id=job_id,
            )

    def _upload_log_file_to_swift(self, job_log_file, job_id) -> None:
        if self._logs_cloud and job_log_file.exists():
            # Due to bug in OTC we need to read the file content
            log_data = open(job_log_file, 'r').read()
            try:
                obj = self._logs_cloud.object_store.create_object(
                    container=self._logs_container_name,
                    name='{id}/{name}'.format(
                        id=job_id,
                        name=job_log_file.name),
                    data=log_data)
                obj.set_metadata(
                    self._logs_cloud.object_store,
                    metadata={
                        'delete-after': str(
                            self.config.get_default(
                                'executor', 'log_swift_keep_time', '1209600')),
                        'content_type': 'text/plain'
                    })
                return True
            except openstack.exceptions.SDKException as e:
                self.log.exception('Error uploading log to Swift')
                if self.alerta:
                    self.alerta.send_alert(
                        severity='major',
                        environment=self.config.alerta_env,
                        origin=self.config.alerta_origin,
                        service=['apimon', 'task_executor'],
                        resource='task',
                        event='LogUpload',
                        value=str(e)
                    )

                return False

    def _flush_clouds_config(self) -> None:
        """Write clouds.yaml to FS into home dir"""
        clouds_loc = Path(Path.home(), '.config', 'openstack')
        clouds_loc.mkdir(parents=True, exist_ok=True)
        with open(Path(clouds_loc, 'clouds.yaml'), 'w') as f:
            yaml.dump(self._clouds_config, f, default_flow_style=False)

        cloud = self.config.get_default('executor', 'logs_cloud', 'swift')
        self._logs_cloud = openstack.connect(cloud=cloud)
        self._logs_cloud.config._statsd_prefix = 'openstack.api.%s.%s' % (
            cloud, self.zone)

        self._logs_container_name = self.config.get_default(
            'executor', 'logs_cloud_container', 'job_logs')
        self._create_logs_container(
            self._logs_cloud, self._logs_container_name
        )

    def execute_job_preparations(self, job) -> None:
        """Basic part of the job"""
        job.sendWorkStatus(0, 100)

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

        name = project_args.get('name')
        type = project_args.get('type')
        repo_url = project_args.get('url')
        repo_ref = project_args.get('ref')
        exec_cmd = project_args.get('exec_cmd')
        work_dir = self.config.get_default('executor', 'work_dir')
        commit = project_args.get('commit')
        rc = 0
        if not project:
            project = Project(
                name=name,
                type=type,
                repo_url=repo_url,
                repo_ref=repo_ref,
                exec_cmd=exec_cmd,
                work_dir=work_dir,
                commit=commit
            )
            project.get_git_repo()
            rc = project.prepare()
            self._projects[project.name] = project
        if (
            str(project.get_commit()) != commit
            or project.repo_ref != repo_ref
            or project.exec_cmd != exec_cmd
        ):
            self.log.debug('current commit is %s' % project.get_commit())
            project.repo_ref = repo_ref
            project.exec_cmd = exec_cmd
            project.refresh_git_repo()
            rc = project.prepare()
        if rc != 0:
            del self._projects[project.name]
        return rc

    def execute_ansible_job(self, job) -> None:
        """Main entry function for ansible job requests"""
        if self.execute_job_preparations(job) != 0:
            job.sendWorkFail()
            return

        self.job_workers[job.unique] = AnsibleJob(self, job)
        self.manage_load()
        if self.statsd:
            self.statsd.incr('apimon.executor.{zone}.{hostname}.ansible_job')

        self.job_workers[job.unique].run()
        # NOTE(gtema): sleep a bit not to accept all jobs immediately on start
        time.sleep(1)

    def execute_misc_job(self, job) -> None:
        """Main entry function for misc job requests"""
        if self.execute_job_preparations(job) != 0:
            job.sendWorkFail()
            return

        self.job_workers[job.unique] = MiscJob(self, job)
        self.manage_load()
        if self.statsd:
            self.statsd.incr('apimon.executor.{zone}.{hostname}.misc_job')

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
                    self.accepting_work = False
                    self.executor_worker.pause()
                    # self.unregister_work()
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
                self.accepting_work = True
                self.executor_worker.resume()
                # self.register_work()
        if self.statsd:
            base_key = 'apimon.executor.{zone}.{hostname}'
            for sensor in self.sensors:
                sensor.reportStats(self.statsd, base_key)

    def finish_job(self, unique) -> None:
        """A callback after the job finished processing"""
        try:
            del(self.job_workers[unique])
        except Exception:
            self.log.error('Trying to delete job data %s, which is not there' %
                           unique)

    # def register_work(self) -> None:
    #     if self._running:
    #         self.accepting_work = True
    #         function_name = 'apimon:ansible'
    #         self.executor_worker.gearman.registerFunction(function_name)
    #         self.executor_worker.resume()
    #         self.executor_worker.gearman._sendGrabJobUniq()

    # def unregister_work(self) -> None:
    #     self.accepting_work = False
    #     function_name = 'apimon:ansible'
    #     self.executor_worker.pause()

    #     self.wait_for_jobs()

    #     self.executor_worker.gearman.unRegisterFunction(function_name)

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
