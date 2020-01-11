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

from concurrent.futures import ThreadPoolExecutor
import configparser
from distutils import dir_util
import gzip
import logging
import os
from pathlib import Path
import random
import shutil
import signal
import string
import subprocess
import tempfile
import threading
import time

import openstack
from openstack import exceptions

try:
    from alertaclient.api import Client as alerta_client
except ImportError:
    alerta_client = None

from apimon_executor.ansible import logconfig
from apimon_executor import queue as _queue


def preexec_function():
    """Pre-exec to hide SIGINT for child process
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class ExecutorLoggingAdapter(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have a
    'task' and 'job_id' key, whose value in brackets is prepended to the log
    message.
    """
    def process(self, msg, kwargs):
        return 'jobId:%s | task:%s | %s' % (
            self.extra.get('job_id'), self.extra.get('task'), msg), kwargs


class TaskItem(object):

    def __init__(self, project, item):
        self.project = project
        self.item = item
        self.name = project.name + '.' + item

    def get_exec_cmd(self):
        return self.project.get_exec_cmd(self.item)

    def is_task_valid(self):
        return self.project.is_task_valid(self.item)


class ApimonScheduler(object):

    log = logging.getLogger('apimon_executor')

    def __init__(self, config):
        self.shutdown_event = threading.Event()
        self.pause_event = threading.Event()
        self.task_queue = _queue.UniqueQueue()
        self.finished_task_queue = _queue.UniqueQueue()
        self._threads = []
        self._reconfigure_event = threading.Event()

        self.config = config

        self.logs_cloud = None
        self.alerta = None

        self._config()

    def _config(self):
        if self.config.log_swift_cloud is not None:
            self.logs_cloud = openstack.connect(self.config.log_swift_cloud)

        if alerta_client:
            alerta_ep = self.config.alerta_endpoint
            alerta_token = self.config.alerta_token
            if alerta_ep and alerta_token:
                self.alerta = alerta_client(
                    endpoint=alerta_ep,
                    key=alerta_token)

    def signal_shutdown(self, signum, frame):
        # Raise shutdown event
        self.log.info('Signal received. Gracefully stop processing')
        self.shutdown_event.set()

    def signal_usr(self, signum, frame):
        # Reconfigure
        self.log.info('USR Signal received. Refreshing')
        self.config.read()
        self._config()
        self._reconfigure_event.set()
        self.shutdown_event.set()

    def create_logs_container(self, connection, container_name):
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
        self.container = container

    def start(self):
        self.log.info('Starting APImon Scheduler')
        work_dir = Path(self.config.work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        if self.logs_cloud and self.config.log_swift_container_name:
            self.create_logs_container(self.logs_cloud,
                                       self.config.log_swift_container_name)
        signal.signal(signal.SIGINT, self.signal_shutdown)
        signal.signal(signal.SIGTERM, self.signal_shutdown)
        signal.signal(signal.SIGUSR1, self.signal_usr)
        self._start_threads()

    def _start_threads(self, reconfig=False):

        if reconfig:
            self.shutdown_event.clear()
            self._reconfigure_event.clear()

        with ThreadPoolExecutor(
                max_workers=self.config.count_executor_threads + 1) \
                as thread_pool:

            for i in range(self.config.count_executor_threads):
                thread = Executor(self.task_queue, self.finished_task_queue,
                                  self.shutdown_event,
                                  self.pause_event,
                                  self.config,
                                  self.logs_cloud,
                                  self.alerta)
                thread_pool.submit(thread.run)
            scheduler_thread = Scheduler(self.task_queue,
                                         self.finished_task_queue,
                                         self.shutdown_event,
                                         self.pause_event,
                                         self.config,
                                         self.alerta)
            thread_pool.submit(scheduler_thread.run)
        if self._reconfigure_event.is_set():
            self.log.info('Restarting threads')
            self._start_threads(reconfig=True)


class Scheduler(object):

    log = logging.getLogger('apimon_executor.scheduler')

    def __init__(self, task_queue, finished_task_queue, shutdown_event,
                 pause_event, config=None, alerta=None):
        self.task_queue = task_queue
        self.finished_task_queue = finished_task_queue
        self.shutdown_event = shutdown_event
        self.pause_event = pause_event
        self.data = threading.local()
        if config:
            self.reconfigure(config)
        self.sleep_time = 1
        self.alerta = alerta

    def reconfigure(self, config):
        self.config = config

    def _init_projects(self):
        for name, project in self.config.projects.items():
            project.prepare()

    def refresh_projects(self):
        current_time = time.time()
        # Check whether git update should be done/checked
        if (self.config.refresh_interval > 0
                and current_time >= self.get_next_refresh_time()):
            for name, project in self.config.projects.items():
                if project.is_repo_update_necessary():
                    # Set pause event for executors not to start new stuff
                    self.pause_event.set()
                    project.refresh_git_repo()
                    project.prepare()
                    # Discard all scheduled items for relevant project
                    for i in range(self.task_queue.qsize()):
                        entity = self.task_queue.get_nowait()
                        if entity.project.name != project.name:
                            # Item of other project - reschedule
                            self.task_queue.put(entity)
                    self.schedule_tasks(self.task_queue, project)
                    # We can continue processing
                    self.pause_event.clear()

            self.set_next_refresh_time(
                time.time() + self.config.refresh_interval)

    def get_next_refresh_time(self):
        return self.data.next_git_refresh

    def set_next_refresh_time(self, time):
        self.data.next_git_refresh = time

    def run(self):
        """Scheduler function

        Reads the git repo and schedule tasks. When an update in the repository
        is detected all tasks are finished, repo updated and new tasks are
        scheduled.
        """
        self.log.info('Starting scheduler thread')
        try:

            self.data = threading.local()

            self._init_projects()

            for name, project in self.config.projects.items():
                self.schedule_tasks(self.task_queue, project)
            self.set_next_refresh_time(
                time.time() + self.config.refresh_interval)
            while not self.shutdown_event.is_set():
                self.refresh_projects()
                # Now check the finished tasks queue and re-queue them
                # Not blocking wait to react on shutdown
                try:
                    task = self.finished_task_queue.get(False, 1)
                    if task:
                        self.task_queue.put(task)
                    else:
                        break
                except _queue.Empty:
                    pass
                if self.alerta:
                    try:
                        self.alerta.heartbeat(
                            origin='task_executor' + self.config.alerta_env,
                            tags=['task_executor', 'scheduler']
                        )
                    except Exception:
                        self.log.exception('Error sending heartbeat')
                time.sleep(self.sleep_time)
        except Exception as e:
            self.log.exception('Error occured in the scheduler thread')
            if self.alerta:
                self.alerta.send_alert(
                    severity='critical',
                    environment=self.config.alerta_env,
                    origin=self.config.alerta_origin,
                    service=['apimon', 'task_executor'],
                    resource='scheduler',
                    event='Exception',
                    value=str(e)
                )

        self.log.info('finishing scheduler thread')
        # If scheduler exits - no sense for executors to remain
        self.shutdown_event.set()

    def schedule_tasks(self, queue, project):
        self.log.debug('Looking for tasks')
        for task in project.tasks():
            queue.put(
                TaskItem(project=project, item=task))


class Executor(object):

    log = logging.getLogger('apimon_executor.executor')

    def __init__(self, task_queue, finished_task_queue, shutdown_event,
                 pause_event, config=None, logs_cloud=None, alerta=None):
        self.task_queue = task_queue
        self.finished_task_queue = finished_task_queue
        self.shutdown_event = shutdown_event
        self.pause_event = pause_event
        if config:
            self.reconfigure(config)
        self.ansible_plugin_path = \
            Path(
                Path(__file__).resolve().parent,
                'ansible', 'callback').as_posix()
        self.logs_cloud = logs_cloud
        self.sleep_time = 3
        self.alerta = alerta

    def reconfigure(self, config):
        self.config = config

    def run(self):
        self.log.info('Starting Executor thread')
        while not self.shutdown_event.is_set():
            if not self.pause_event.is_set():
                # Not blocking wait to be able to react on shutdown
                try:
                    task_item = self.task_queue.get(False, 1)
                    if task_item:
                        self.log.debug('Fetched item=%s', task_item.item)
                        if not task_item.is_task_valid():
                            # We might have gotten task_item, which does not
                            # exist in the repo anymore (after repo refresh).
                            # Just inform and continue
                            self.log.info('item %s is not valid, since it '
                                          'doesn\'t exist anymore. Skipping' %
                                          task_item.name)
                            continue
                        cmd = task_item.get_exec_cmd()
                        # job_id = str(uuid.uuid4().hex[-12:])
                        job_id = ''.join([random.choice(string.ascii_letters +
                                                        string.digits) for n in
                                          range(12)])
                        self.log.info('Starting task %s with jobId: %s', cmd,
                                      job_id)
                        try:
                            if not self.config.simulate:
                                self.execute(task_item, job_id)
                            else:
                                # Simulate processing
                                time.sleep(1)
                            self.log.info('Task %s (%s) finished', cmd, job_id)
                        except Exception as e:
                            self.log.exception(e)
                            try:
                                if self.alerta:
                                    self.alerta.send_alert(
                                        severity='major',
                                        environment=self.config.alerta_env,
                                        origin=self.config.alerta_origin,
                                        service=['apimon', 'task_executor'],
                                        resource='task',
                                        event='Exception',
                                        value=str(e)
                                    )
                            except Exception:
                                self.log.exception('Error sending data to '
                                                   'alerta')
                        finally:
                            # Even if we had a bad exception during job
                            # execution, try not to loose item and reschedule
                            # it
                            if not self.pause_event.is_set():
                                self.finished_task_queue.put(task_item)
                            else:
                                self.log.debug('Finishing entity while '
                                               'pause_event is set - not '
                                               'rescheduling.')
                            self.task_queue.task_done()
                    else:
                        break
                except _queue.Empty:
                    pass
                except Exception as e:
                    self.log.exception('Exception occured during task '
                                       'processing')
                    try:
                        if self.alerta:
                            self.alerta.send_alert(
                                severity='critical',
                                environment=self.config.alerta_env,
                                origin=self.config.alerta_origin,
                                service=['apimon', 'task_executor'],
                                resource='task',
                                event='Exception',
                                value=str(e)
                            )
                    except Exception:
                        self.log.exception('Error sending data to '
                                           'alerta')
            time.sleep(self.sleep_time)
        self.log.info('Finishing executor thread')

    def archive_log_file(self, job_log_file):
        if job_log_file.exists():
            # Archive log
            with open(job_log_file, 'rb') as f_in:
                with gzip.open(
                        job_log_file.with_suffix('.txt.gz'), 'wb') as gz:
                    shutil.copyfileobj(f_in, gz)

            # Now remove the job log, since we archived it
            job_log_file.unlink()

    def upload_log_file_to_swift(self, job_log_file, job_id):
        if self.logs_cloud and job_log_file.exists():
            # Due to bug in OTC we need to read the file content
            log_data = open(job_log_file, 'r').read()
            try:
                obj = self.logs_cloud.object_store.create_object(
                    container=self.config.log_swift_container_name,
                    name='{id}/{name}'.format(
                        id=job_id,
                        name=job_log_file.name),
                    data=log_data)
                obj.set_metadata(
                    self.logs_cloud.object_store,
                    metadata={
                        'delete-after': str(self.config.log_swift_keep_time),
                        'content_type': 'text/plain'
                    })
                return True
            except exceptions.SDKException as e:
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

    def execute(self, task_item, job_id=None):
        """Execute the command
        """
        task_logger = logging.getLogger('apimon_executor.executor.task')
        adapter = ExecutorLoggingAdapter(task_logger, {'task': task_item.item,
                                                       'job_id': job_id})
        with tempfile.TemporaryDirectory() as tmpdir:
            job_work_dir = Path(tmpdir, 'work')
            # Copy work_dir into job_work_dir
            dir_util.copy_tree(task_item.project.project_dir,
                               job_work_dir.as_posix(),
                               preserve_symlinks=1)
            # Prepare dir for job logs
            job_log_dir = Path(self.config.log_dest, str(job_id[-2:]), job_id)
            job_log_dir.mkdir(parents=True, exist_ok=True)
            # Generate job log config
            job_log_file = Path(job_log_dir, 'job-output.txt')
            self.log.debug('Saving job output in %s', job_log_file)
            job_log_config = logconfig.JobLoggingConfig(
                job_output_file=job_log_file.as_posix())
            # Flush job log config into file for plugin to fetch it
            job_log_config_file = Path(job_work_dir, 'logging.json').as_posix()
            job_log_config.writeJson(job_log_config_file)

            self._prepare_ansible_cfg(job_work_dir)

            execute_cmd = (task_item.get_exec_cmd()).split(' ')

            # Adapt job env
            env = os.environ.copy()
            env['TASK_EXECUTOR_JOB_ID'] = job_id
            env['ANSIBLE_CALLBACK_PLUGINS'] = \
                self.ansible_plugin_path
            env['APIMON_EXECUTOR_JOB_CONFIG'] = job_log_config_file
            for k, v in task_item.project.env.items():
                env[k] = v

            process = subprocess.Popen(execute_cmd, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       preexec_fn=preexec_function,
                                       env=env,
                                       cwd=job_work_dir,
                                       restore_signals=False)

            # Read the output
            for line in process.stdout:
                adapter.debug('%s', line.decode('utf-8'))
            stderr = process.stderr.read()
            if stderr:
                adapter.error('%s', stderr.decode('utf-8'))

            # Wait for child process to finish
            process.wait()

            res = self.upload_log_file_to_swift(job_log_file, job_id)

            if self.config.log_fs_keep or not res:
                if self.config.log_fs_archive:
                    self.archive_log_file(job_log_file)
            else:
                try:
                    job_log_file.unlink()
                    job_log_dir.rmdir()
                except Exception:
                    pass
