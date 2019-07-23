#!/usr/bin/env python3

import argparse
from concurrent.futures import ThreadPoolExecutor
import queue as _queue
import logging
import os
from pathlib import Path
import signal
import subprocess
import threading
import time

from git import Repo


shutdown_event = threading.Event()
pause_event = threading.Event()
discard_tasks_event = threading.Event()

logger = logging.getLogger(__name__)
log_handler = None


def executor(task_queue, finished_task_queue, execute_cmd, task_location,
             simulate=False):
    logging.info('Starting executor thread')
    while not shutdown_event.is_set():
        if not discard_tasks_event.is_set():
            # Not blocking wait to be able to react on shutdown
            try:
                task_item = task_queue.get(False, 1)
                if task_item:
                    cmd = execute_cmd % task_item
                    logging.info('Starting task %s', cmd)
                    if not simulate:
                        execute(execute_cmd, task_item, task_location)
                        pass
                    else:
                        # Simulate processing
                        time.sleep(1)
                    logging.info('Task %s finished', cmd)
                    finished_task_queue.put(task_item)
                    task_queue.task_done()
                else:
                    break
            except _queue.Empty:
                pass
            except Exception as e:
                logging.fatal('Exception occured during task processing: %s',
                              e,
                              exc_info=True)
        time.sleep(3)
    logging.info('Finishing executor thread')


def preexec_function():
    """Pre-exec to hide SIGINT for child process
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def execute(cmd, task, task_location):
    """Execute the command
    """
    task_logger = logging.getLogger('executor.task')
    task_logger.setLevel(logging.INFO)

    # add ch to logger
    task_logger.addHandler(get_log_handler())

    execute_cmd = (cmd % Path(task_location, task)).split(' ')

    process = subprocess.Popen(execute_cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               preexec_fn=preexec_function,
                               restore_signals=False)
    # Read the output
    for line in process.stdout:
        task_logger.info('%s: %s', task, line.decode('utf-8'))
    if process.stderr:
        task_logger.error('%s: %s', task,
                          process.stderr.read().decode('utf-8'))


def discard_queue_elements(queue):
    """Simply discard all event from queue
    """
    logging.debug('Discarding task from the task_queue due to discard '
                  'event being set')
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
    logging.info('Starting scheduler thread')
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
                    logging.debug('Waiting for the current queue to become '
                                  'empty')
                    discard_tasks_event.set()
                    discard_queue_elements(task_queue)
                    discard_queue_elements(finished_task_queue)
                    task_queue.join()
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
    except Exception as e:
        logging.fatal('Error occured in the scheduler thread: %s' % e,
                      exc_info=True)
    logging.info('finishing scheduler thread')


def get_git_repo(repo_url, work_dir, ref):
    """Get a repository object
    """
    logging.debug('Getting git repository')
    git_path = Path(work_dir, '.git')
    if git_path.exists():
        repo = Repo(work_dir)
        refresh_git_repo(repo, ref)
    else:
        repo = Repo.clone_from(repo_url, work_dir)
        repo.remotes.origin.pull(ref)
    return repo


def is_repo_update_necessary(repo, ref):
    """Check whether update of the repository is necessary

    Returns True, when a commit checksum remotely differs from local state
    """
    logging.debug('Checking whether there are remote changes in git')
    repo.remotes.origin.update()
    last_commit_remote = repo.remotes.origin.refs[ref].commit
    last_commit_local = repo.refs[ref].commit
    return last_commit_remote != last_commit_local


def refresh_git_repo(repo, ref):
    repo.remotes.origin.pull(ref)


def schedule_tasks(queue, work_dir, location):
    logging.debug('Looking for tasks')
    for file in Path(work_dir, location).glob('scenario*.yaml'):
        logging.debug('Scheduling %s' % file)
        queue.put(file)


def signal_handler(signum, frame):
    # Raise shutdown event
    logging.info('Signal received. Gracefully stop processing')
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

    # add ch to logger
    logger.addHandler(get_log_handler())

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'repo',
        help='URL of the repository to get scenarios from.'
    )
    parser.add_argument(
        '--location',
        help='Location in the repository to get playbooks from.',
        default='/playbooks/scenarios'
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
    logging.info('starting')
    curr_dir = os.getcwd()
    try:
        os.chdir(args.work_dir)
        with ThreadPoolExecutor(max_workers=args.count_executor_threads + 1) \
                as thread_pool:
            signal.signal(signal.SIGINT, signal_handler)
            task_queue = _queue.Queue()
            finished_task_queue = _queue.Queue()

            for i in range(args.count_executor_threads):
                thread_pool.submit(executor, task_queue, finished_task_queue,
                                   args.command, args.location, args.simulate)
            thread_pool.submit(scheduler, task_queue, finished_task_queue,
                               args.repo, args.location, args.work_dir,
                               args.ref, args.interval)

    finally:
        os.chdir(curr_dir)

    return


if __name__ == "__main__":
    main()
