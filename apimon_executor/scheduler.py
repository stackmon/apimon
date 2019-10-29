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

        self.config = config

    def signal_handler(self, signum, frame):
        # Raise shutdown event
        self.log.info('Signal received. Gracefully stop processing')
        self.shutdown_event.set()

    def start(self):
        self.log.info('Starting APImon Scheduler')
        work_dir = Path(self.config.work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        with ThreadPoolExecutor(
                max_workers=self.config.count_executor_threads + 1) \
                as thread_pool:
            signal.signal(signal.SIGINT, self.signal_handler)

            for i in range(self.config.count_executor_threads):
                thread = Executor(self.task_queue, self.finished_task_queue,
                                  self.shutdown_event,
                                  self.pause_event)
                thread.reconfigure(self.config)
                thread_pool.submit(thread.run)
            scheduler_thread = Scheduler(self.task_queue,
                                         self.finished_task_queue,
                                         self.shutdown_event,
                                         self.pause_event)
            scheduler_thread.reconfigure(self.config)
            thread_pool.submit(scheduler_thread.run)


class Scheduler(object):

    log = logging.getLogger('apimon_executor.scheduler')

    def __init__(self, task_queue, finished_task_queue, shutdown_event,
                 pause_event, config=None):
        self.task_queue = task_queue
        self.finished_task_queue = finished_task_queue
        self.shutdown_event = shutdown_event
        self.pause_event = pause_event
        self.data = threading.local()
        if config:
            self.reconfigure(config)
        self.sleep_time = 1

    def reconfigure(self, config):
        for k, v in config.__dict__.items():
            setattr(self, '_' + k, v)

    def _init_projects(self):
        for name, project in self._projects.items():
            project.prepare()

    def refresh_projects(self):
        current_time = time.time()
        # Check whether git update should be done/checked
        if (self._refresh_interval > 0
                and current_time >= self.get_next_refresh_time()):
            for name, project in self._projects.items():
                if project.is_repo_update_necessary():
                    # Set pause event for executors not to start new stuff
                    self.pause_event.set()
                    project.refresh_git_repo()
                    self.schedule_tasks(self.task_queue, project)
                    # We can continue processing
                    self.pause_event.clear()

            self.set_next_refresh_time(time.time() + self._refresh_interval)

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

            for name, project in self._projects.items():
                self.schedule_tasks(self.task_queue, project)
            self.set_next_refresh_time(time.time() + self._refresh_interval)
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
                time.sleep(self.sleep_time)
        except Exception:
            self.log.exception('Error occured in the scheduler thread')
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
                 pause_event, config=None):
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
        self.sleep_time = 3

    def reconfigure(self, config):
        for k, v in config.__dict__.items():
            setattr(self, '_' + k, v)

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
                            if not self._simulate:
                                self.execute(task_item, job_id)
                            else:
                                # Simulate processing
                                time.sleep(1)
                            self.log.info('Task %s (%s) finished', cmd, job_id)
                        except Exception as e:
                            self.log.exception(e)
                        finally:
                            # Even if we had a bad exception during job
                            # execution, try not to loose item and reschedule
                            # it
                            self.finished_task_queue.put(task_item)
                            self.task_queue.task_done()
                    else:
                        break
                except _queue.Empty:
                    pass
                except Exception:
                    self.log.exception('Exception occured during task '
                                       'processing')
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
            job_log_dir = Path(self._log_dest, str(job_id[-2:]), job_id)
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

            self.archive_log_file(job_log_file)
