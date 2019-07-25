#!/usr/bin/env python3

import argparse
from concurrent.futures import ThreadPoolExecutor
import queue as _queue
import logging
import os
from pathlib import Path
import random
import signal
import string
import subprocess
import threading
import time
# import uuid

from git import Repo


shutdown_event = threading.Event()
pause_event = threading.Event()
discard_tasks_event = threading.Event()

logger = logging.getLogger('executor')
task_logger = logging.getLogger('executor.task')
log_handler = None


class ExecutorLoggingAdapter(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have a
    'task' and 'job_id' key, whose value in brackets is prepended to the log
    message.
    """
    def process(self, msg, kwargs):
        return 'jobId:%s | task:%s | %s' % (
            self.extra.get('job_id'), self.extra.get('task'), msg), kwargs


def executor(task_queue, finished_task_queue, execute_cmd, simulate=False):
    logger.info('Starting executor thread')
    while not shutdown_event.is_set():
        if not discard_tasks_event.is_set():
            # Not blocking wait to be able to react on shutdown
            try:
                task_item = task_queue.get(False, 1)
                if task_item:
                    cmd = execute_cmd % task_item
                    # job_id = str(uuid.uuid4().hex[-12:])
                    job_id = ''.join([random.choice(string.ascii_letters +
                                                    string.digits) for n in
                                      range(12)])
                    logging.info('Starting task %s with jobId: %s', cmd,
                                 job_id)
                    if not simulate:
                        execute(execute_cmd, task_item, job_id)
                        pass
                    else:
                        # Simulate processing
                        time.sleep(1)
                    logging.info('Task %s (%s) finished', cmd, job_id)
                    finished_task_queue.put(task_item)
                    task_queue.task_done()
                else:
                    break
            except _queue.Empty:
                pass
            except Exception:
                logger.exception('Exception occured during task processing')
        time.sleep(3)
    logger.info('Finishing executor thread')


def preexec_function():
    """Pre-exec to hide SIGINT for child process
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def execute(cmd, task, job_id=None):
    """Execute the command
    """
    adapter = ExecutorLoggingAdapter(task_logger, {'task': task, 'job_id':
                                                   job_id})
    execute_cmd = (cmd % Path(task)).split(' ')

    env = os.environ.copy()

    env['TASK_EXECUTOR_JOB_ID'] = job_id

    process = subprocess.Popen(execute_cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               preexec_fn=preexec_function,
                               env=env,
                               restore_signals=False)
    # Read the output
    for line in process.stdout:
        adapter.info('%s', line.decode('utf-8'))
    stderr = process.stderr.read()
    if stderr:
        adapter.error('%s', stderr.decode('utf-8'))


def discard_queue_elements(queue):
    """Simply discard all event from queue
    """
    logger.debug('Discarding task from the %s due to discard '
                 'event being set', queue)
    while not queue.empty():
        try:
            queue.get(False)
            queue.task_done()
        except _queue.Empty:
            pass


def scheduler(task_queue, finished_task_queue, repo, location, work_dir, ref,
              repo_refresh_interval):
    """Scheduler function

    Reads the git repo and schedule tasks. When an update in the repository is
    detected all tasks are finished, repo updated and new tasks are scheduled.
    """
    logger.info('Starting scheduler thread')
    try:
        data = threading.local()
        git_repo = get_git_repo(repo, work_dir, ref)
        schedule_tasks(task_queue, work_dir, location)
        data.next_git_refresh = time.time() + repo_refresh_interval
        while not shutdown_event.is_set():
            current_time = time.time()
            # Check whether git update should be done/checked
            if current_time >= data.next_git_refresh:
                if is_repo_update_necessary(git_repo, ref):
                    # Check for all current scheduled items to complete
                    logger.debug('Waiting for the current queue to become '
                                 'empty')
                    discard_tasks_event.set()
                    discard_queue_elements(task_queue)
                    task_queue.join()
                    # wait for all running tasks to complete
                    # and then also clean the finished queue
                    discard_queue_elements(finished_task_queue)
                    discard_tasks_event.clear()
                    # Refresh the work_dir
                    refresh_git_repo(git_repo, ref)
                    schedule_tasks(task_queue, work_dir, location)
                    data.next_git_refresh = time.time() + repo_refresh_interval
            # Now check the finished tasks queue and re-queue them
            # Not blocking wait to react on shutdown
            try:
                task = finished_task_queue.get(False, 1)
                if task:
                    task_queue.put(task)
                else:
                    break
            except _queue.Empty:
                pass
            time.sleep(1)
    except Exception:
        logger.exception('Error occured in the scheduler thread')
    logger.info('finishing scheduler thread')
    shutdown_event.set()


def get_git_repo(repo_url, work_dir, ref):
    """Get a repository object
    """
    logger.debug('Getting git repository')
    git_path = Path(work_dir, '.git')
    if git_path.exists():
        repo = Repo(work_dir)
        refresh_git_repo(repo, ref)
    else:
        repo = Repo.clone_from(repo_url, work_dir, recurse_submodules='.')
        repo.remotes.origin.pull(ref)
    return repo


def is_repo_update_necessary(repo, ref):
    """Check whether update of the repository is necessary

    Returns True, when a commit checksum remotely differs from local state
    """
    logger.debug('Checking whether there are remote changes in git')
    repo.remotes.origin.update()
    last_commit_remote = repo.remotes.origin.refs[ref].commit
    last_commit_local = repo.refs[ref].commit
    return last_commit_remote != last_commit_local


def refresh_git_repo(repo, ref):
    repo.remotes.origin.pull(ref, recurse_submodules=True)


def schedule_tasks(queue, work_dir, location):
    logger.debug('Looking for tasks')
    for file in Path(work_dir, location).glob('scenario*.yaml'):
        logger.debug('Scheduling %s' % file)
        queue.put(file)


def signal_handler(signum, frame):
    # Raise shutdown event
    logger.info('Signal received. Gracefully stop processing')
    shutdown_event.set()


def get_log_handler():
    """Get a log handler
    """
    global log_handler
    if log_handler:
        return log_handler
    # create console handler and set level to debug
    log_handler = logging.StreamHandler()
    log_handler.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(message)s')

    # add formatter to ch
    log_handler.setFormatter(formatter)
    return log_handler


def main():
    logger.setLevel(logging.DEBUG)
    task_logger.setLevel(logging.INFO)

    # add ch to logger
    logger.addHandler(get_log_handler())
    # add ch to logger
    task_logger.addHandler(get_log_handler())

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'repo',
        help='URL of the repository to get scenarios from.'
    )
    parser.add_argument(
        '--location',
        help='Location in the repository to get playbooks from.',
        default='playbooks/scenarios'
    )
    parser.add_argument(
        '--work_dir',
        default='.',
        help='Working diretory to check the repository out.'
    )
    parser.add_argument(
        '--ref',
        default='master',
        help='Git reference to use.'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=120,
        help='Interval in seconds to check repository for updates.'
    )
    parser.add_argument(
        '--count_executor_threads',
        type=int,
        default=5,
        help='Count of the executor threads.'
    )
    parser.add_argument(
        '--command',
        default='ansible-playbook -i inventory/testing %s',
        help='Command to be executed.'
    )
    parser.add_argument(
        '--simulate',
        type=bool,
        default=False,
        help='Simulate execution.'
    )
    args = parser.parse_args()
    logger.info('starting')
    curr_dir = os.getcwd()
    try:
        Path(args.work_dir).mkdir(parents=True, exist_ok=True)
        os.chdir(args.work_dir)
        with ThreadPoolExecutor(max_workers=args.count_executor_threads + 1) \
                as thread_pool:
            signal.signal(signal.SIGINT, signal_handler)
            task_queue = _queue.Queue()
            finished_task_queue = _queue.Queue()

            for i in range(args.count_executor_threads):
                thread_pool.submit(executor, task_queue, finished_task_queue,
                                   args.command, args.simulate)
            thread_pool.submit(scheduler, task_queue, finished_task_queue,
                               args.repo, args.location, args.work_dir,
                               args.ref, args.interval)

    finally:
        os.chdir(curr_dir)

    return


if __name__ == "__main__":
    main()
