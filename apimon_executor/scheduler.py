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
import gzip
import queue as _queue
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

from git import Repo

from apimon_executor.ansible import logconfig


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


def discard_queue_elements(self, queue):
    """Simply discard all event from queue
    """
    self.log.debug('Discarding task from the %s due to discard '
                   'event being set', queue)
    while not queue.empty():
        try:
            queue.get(False)
            queue.task_done()
        except _queue.Empty:
            pass


class ApimonScheduler(object):

    log = logging.getLogger('apimon_executor')

    def __init__(self, config):
        self.shutdown_event = threading.Event()
        self.pause_event = threading.Event()
        self.discard_tasks_event = threading.Event()
        self.task_queue = _queue.Queue()
        self.finished_task_queue = _queue.Queue()

        self.config = config

    def signal_handler(self, signum, frame):
        # Raise shutdown event
        self.log.info('Signal received. Gracefully stop processing')
        self.shutdown_event.set()

    def start(self):
        self.log.info('Starting APImon Scheduler')
        git_checkout_dir = Path(self.config.git_checkout_dir)
        git_checkout_dir.mkdir(parents=True, exist_ok=True)
        with ThreadPoolExecutor(
                max_workers=self.config.count_executor_threads + 1) \
                as thread_pool:
            signal.signal(signal.SIGINT, self.signal_handler)

            for i in range(self.config.count_executor_threads):
                thread = Executor(self.task_queue, self.finished_task_queue,
                                  self.shutdown_event,
                                  self.discard_tasks_event)
                thread.reconfigure(self.config)
                thread_pool.submit(thread.run)
            scheduler_thread = Scheduler(self.task_queue,
                                         self.finished_task_queue,
                                         self.shutdown_event,
                                         self.discard_tasks_event)
            scheduler_thread.reconfigure(self.config)
            thread_pool.submit(scheduler_thread.run)


class Scheduler(object):

    log = logging.getLogger('apimon_executor.scheduler')

    def __init__(self, task_queue, finished_task_queue, shutdown_event,
                 ignore_tasks_event, config=None):
        self.task_queue = task_queue
        self.finished_task_queue = finished_task_queue
        self.shutdown_event = shutdown_event
        self.ignore_tasks_event = ignore_tasks_event
        if config:
            self.reconfigure(config)

    def reconfigure(self, config):
        for k, v in config.__dict__.items():
            setattr(self, '_' + k, v)

    def discard_queue_elements(self, queue):
        """Simply discard all event from queue
        """
        self.log.debug('Discarding task from the %s due to discard '
                       'event being set', queue)
        while not queue.empty():
            try:
                queue.get(False)
                queue.task_done()
            except _queue.Empty:
                pass

    def run(self):
        """Scheduler function

        Reads the git repo and schedule tasks. When an update in the repository
        is detected all tasks are finished, repo updated and new tasks are
        scheduled.
        """
        self.log.info('Starting scheduler thread')
        try:
            data = threading.local()

            git_repo = self.get_git_repo(self._repo,
                                         self._git_checkout_dir, self._git_ref)

            self.schedule_tasks(self.task_queue)
            data.next_git_refresh = time.time() + \
                self._git_refresh_interval
            while not self.shutdown_event.is_set():
                current_time = time.time()
                # Check whether git update should be done/checked
                if (self._git_refresh_interval > 0
                        and current_time >= data.next_git_refresh):
                    if self.is_repo_update_necessary(git_repo,
                                                     self._git_ref):
                        # Check for all current scheduled items to complete
                        self.log.debug('Waiting for the current queue to '
                                       'become empty')
                        self.discard_tasks_event.set()
                        self.discard_queue_elements(self.task_queue)
                        self.task_queue.join()
                        # wait for all running tasks to complete
                        # and then also clean the finished queue
                        self.discard_queue_elements(self.finished_task_queue)
                        self.discard_tasks_event.clear()
                        # Refresh the work_dir
                        self.refresh_git_repo(git_repo, self._git_ref)
                        self.schedule_tasks(self.task_queue)
                        data.next_git_refresh = time.time() + \
                            self._repo_refresh_interval
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
                time.sleep(1)
        except Exception:
            self.log.exception('Error occured in the scheduler thread')
        self.log.info('finishing scheduler thread')
        # If scheduler exits - no sense for executors to remain
        self.shutdown_event.set()

    def get_git_repo(self, repo_url, work_dir, ref):
        """Get a repository object
        """
        self.log.debug('Getting git repository')
        git_path = Path(work_dir, '.git')
        if git_path.exists():
            repo = Repo(work_dir)
            self.refresh_git_repo(repo, ref)
        else:
            repo = Repo.clone_from(repo_url, work_dir, recurse_submodules='.')
            repo.remotes.origin.pull(ref)
        return repo

    def is_repo_update_necessary(self, repo, ref):
        """Check whether update of the repository is necessary

        Returns True, when a commit checksum remotely differs from local state
        """
        self.log.debug('Checking whether there are remote changes in git')
        try:
            repo.remotes.origin.update()
            last_commit_remote = repo.remotes.origin.refs[ref].commit
            last_commit_local = repo.refs[ref].commit
            return last_commit_remote != last_commit_local
        except Exception:
            self.log.exception('Cannot update git remote')
            return False

    def refresh_git_repo(self, repo, ref):
        try:
            repo.remotes.origin.pull(ref, recurse_submodules=True)
        except Exception:
            self.log.exception('Cannot update repository')

    def schedule_tasks(self, queue):
        self.log.debug('Looking for tasks')
        if self._scenarios:
            for scenario in self._scenarios:
                scenario_file = Path(self._git_checkout_dir,
                                     self._location, scenario)
                if scenario_file.exists():
                    queue.put(
                        scenario_file.relative_to(self._git_checkout_dir))
                else:
                    self.log.warning('Requested scenario %s does not exist',
                                     scenario)
        else:
            for file in Path(
                    self._git_checkout_dir,
                    self._location).glob('scenario*.yaml'):
                self.log.debug('Scheduling %s', file)
                queue.put(file.relative_to(self._git_checkout_dir))


class Executor(object):

    log = logging.getLogger('apimon_executor.executor')

    def __init__(self, task_queue, finished_task_queue, shutdown_event,
                 ignore_tasks_event, config=None):
        self.task_queue = task_queue
        self.finished_task_queue = finished_task_queue
        self.shutdown_event = shutdown_event
        self.ignore_tasks_event = ignore_tasks_event
        if config:
            self.reconfigure(config)
        self.ansible_plugin_path = \
            Path(
                Path(__file__).resolve().parent,
                'ansible', 'callback').as_posix()

    def reconfigure(self, config):
        for k, v in config.__dict__.items():
            setattr(self, '_' + k, v)

    def discard_queue_elements(self, queue):
        """Simply discard all event from queue
        """
        self.log.debug('Discarding task from the %s due to discard '
                       'event being set', queue)
        while not queue.empty():
            try:
                queue.get(False)
                queue.task_done()
            except _queue.Empty:
                pass

    def run(self):
        self.log.info('Starting Executor thread')
        while not self.shutdown_event.is_set():
            if not self.ignore_tasks_event.is_set():
                # Not blocking wait to be able to react on shutdown
                try:
                    task_item = self.task_queue.get(False, 1)
                    if task_item:
                        self.log.debug('Fetched item=%s', task_item)
                        cmd = self._exec_cmd % task_item
                        # job_id = str(uuid.uuid4().hex[-12:])
                        job_id = ''.join([random.choice(string.ascii_letters +
                                                        string.digits) for n in
                                          range(12)])
                        self.log.info('Starting task %s with jobId: %s', cmd,
                                      job_id)
                        if not self._simulate:
                            self.execute(self._exec_cmd, task_item,
                                         job_id)
                            pass
                        else:
                            # Simulate processing
                            time.sleep(1)
                        self.log.info('Task %s (%s) finished', cmd, job_id)
                        self.finished_task_queue.put(task_item)
                        self.task_queue.task_done()
                    else:
                        break
                except _queue.Empty:
                    pass
                except Exception:
                    self.log.exception('Exception occured during task '
                                       'processing')
            time.sleep(3)
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

    def execute(self, cmd, task, job_id=None):
        """Execute the command
        """
        task_logger = logging.getLogger('apimon_executor.executor.task')
        adapter = ExecutorLoggingAdapter(task_logger, {'task': task, 'job_id':
                                                       job_id})
        with tempfile.TemporaryDirectory() as tmpdir:
            job_work_dir = Path(tmpdir, 'work')
            # Copy work_dir into job_work_dir
            shutil.copytree(self._git_checkout_dir, job_work_dir,
                            symlinks=True)
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

            execute_cmd = (cmd % Path(task)).split(' ')

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
