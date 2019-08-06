import os
import copy
from time import sleep
from pathlib import Path
import configparser
import tempfile
import unittest
from unittest import mock
from concurrent.futures import ThreadPoolExecutor
import time
from apimon_executor import scheduler as _scheduler
from apimon_executor import config as _config
import subprocess
from random import randrange

_timeout = time.time() + 60


class ExecutorTest(unittest.TestCase):
    def setUp(self):
        self._cnf = _config.ExecutorConfig(args={})
        ApimonScheduler = _scheduler.ApimonScheduler(None)
        self.shutdown_event = ApimonScheduler.shutdown_event
        self.ignore_tasks_event = ApimonScheduler.ignore_tasks_event
        self.task_queue = ApimonScheduler.task_queue
        self.finished_task_queue = ApimonScheduler.finished_task_queue

    def test_prepare_ansible_cfg_no_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir, 'ansible.cfg')

            self.assertFalse(config_file.exists())
            executor = _scheduler.Executor(None, None, None, None)

            executor._prepare_ansible_cfg(tmpdir)

            self.assertTrue(config_file.exists())

            config = configparser.ConfigParser()

            config.read(config_file)

            self.assertEqual(config['defaults']['stdout_callback'],
                             'apimon_logger')

    def test_prepare_ansible_cfg_override_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:

            config_file = Path(tmpdir, 'ansible.cfg')

            precond = {
                'defaults': {
                    'stdout_callback': 'fake',
                    'callback_whitelist': 'a1,a2,a3'
                },
                'fake_section': {
                    'fake_key': 'fake_val'
                }
            }

            executor = _scheduler.Executor(None, None, None, None)

            config = configparser.ConfigParser()
            config.read_dict(precond)
            with open(Path(tmpdir, 'ansible.cfg'), 'w') as f:
                config.write(f)

            executor._prepare_ansible_cfg(tmpdir)

            self.assertTrue(config_file.exists())

            config = configparser.ConfigParser()

            config.read(config_file)

            actual = {s: dict(config.items(s)) for s in config.sections()}

            expected = copy.deepcopy(precond)
            expected['defaults']['stdout_callback'] = 'apimon_logger'

            self.assertDictEqual(actual, expected)

    def _dummy_execute(self, cmd, task):
        command = "sleep {}".format(randrange(5, 9))
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            shell=True,
            stderr=subprocess.STDOUT)
        process.wait()
        if time.time() > _timeout:
            self.shutdown_event.set()

    @mock.patch(
        "apimon_executor.scheduler.Scheduler.get_git_repo",
        mock.MagicMock(
            return_value="test_repo"))
    @mock.patch(
        "apimon_executor.scheduler.Scheduler.is_repo_update_necessary",
        return_value=False)
    @mock.patch(
        "apimon_executor.scheduler.Executor.execute")
    def test_executor_run(
        self,
        mock_repo_update_necessary,
        mock_cmd_execute):

        cnf = self._cnf
        mock_cmd_execute.side_effect = self._dummy_execute
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(Path(tmpdir, 'dummy'))
            Path(tmpdir, 'dummy/scenario1.yaml').touch()
            Path(tmpdir, 'dummy/scenario2.yaml').touch()
            setattr(cnf, 'scenarios', ['scenario1.yaml', 'scenario2.yaml'])
            setattr(cnf, 'location', 'dummy')
            setattr(cnf, 'git_checkout_dir', tmpdir)
            setattr(cnf, 'git_refresh_interval', 6)

            scheduler = _scheduler.Scheduler(
                self.task_queue,
                self.finished_task_queue,
                self.shutdown_event,
                self.ignore_tasks_event,
                cnf)
            executor = _scheduler.Executor(
                self.task_queue,
                self.finished_task_queue,
                self.shutdown_event,
                self.ignore_tasks_event,
                cnf)
            with ThreadPoolExecutor(max_workers=(
                cnf.count_executor_threads + 1)) as thread_pool:
                scheduler.reconfigure(cnf)
                thread_pool.submit(scheduler.run)
                for i in range(cnf.count_executor_threads):
                    executor.reconfigure(cnf)


class SchedulerTest(unittest.TestCase):
    def setUp(self):
        self._cnf = _config.ExecutorConfig(args={})
        ApimonScheduler = _scheduler.ApimonScheduler(None)
        self.shutdown_event = ApimonScheduler.shutdown_event
        self.ignore_tasks_event = ApimonScheduler.ignore_tasks_event
        self.task_queue = ApimonScheduler.task_queue
        self.finished_task_queue = ApimonScheduler.finished_task_queue

    def test_schedule_tasks(self):
        cnf = self._cnf
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(Path(tmpdir, 'dummy'))
            Path(tmpdir, 'dummy/scenario1.yaml').touch()
            Path(tmpdir, 'dummy/scenario2.yaml').touch()
            setattr(cnf, 'scenarios', ['scenario1.yaml', 'scenario2.yaml'])
            setattr(cnf, 'location', 'dummy')
            setattr(cnf, 'git_checkout_dir', tmpdir)
            last_qsize = self.task_queue.qsize()
            scheduler = _scheduler.Scheduler(None, None, None, None, cnf)
            scheduler.schedule_tasks(self.task_queue)
            self.assertTrue(self.task_queue.qsize() > last_qsize)
            scheduler.discard_queue_elements(self.task_queue)

    def test_discard_queue_tasks(self):
        self.task_queue.put('task1')
        self.task_queue.put('task1')
        self.assertTrue(self.task_queue.qsize() != 0)
        scheduler = _scheduler.Scheduler(None, None, None, None)
        scheduler.discard_queue_elements(self.task_queue)
        self.assertTrue(self.task_queue.qsize() == 0)

    @mock.patch(
        "apimon_executor.scheduler.Scheduler.get_git_repo",
        mock.MagicMock(
            return_value="test_repo"))
    @mock.patch(
        "apimon_executor.scheduler.Scheduler.is_repo_update_necessary")
    @mock.patch(
        "apimon_executor.scheduler.Scheduler.refresh_git_repo")
    def test_scheduler_run(
        self,
        mock_repo_update_necessary,
        mock_refresh_repo):
        cnf = self._cnf
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(Path(tmpdir, 'dummy'))
            Path(tmpdir, 'dummy/scenario1.yaml').touch()
            Path(tmpdir, 'dummy/scenario2.yaml').touch()
            setattr(cnf, 'scenarios', ['scenario1.yaml', 'scenario2.yaml'])
            setattr(cnf, 'location', 'dummy')
            setattr(cnf, 'git_checkout_dir', tmpdir)
            setattr(cnf, 'git_refresh_interval', 6)
            scheduler = _scheduler.Scheduler(
                self.task_queue,
                self.finished_task_queue,
                self.shutdown_event,
                self.ignore_tasks_event,
                cnf)
            with ThreadPoolExecutor(max_workers=1) as thread_pool:
                mock_repo_update_necessary.return_value = False
                thread_pool.submit(scheduler.run)
                sleep(0.2)
                self.assertFalse(mock_refresh_repo.called)
                self.assertTrue(self.task_queue.qsize() != 0)
                mock_repo_update_necessary.return_value = True
                sleep(cnf.git_refresh_interval + 1)
                self.assertTrue(mock_refresh_repo.called)
                self.assertTrue(self.task_queue.qsize() != 0)
                self.shutdown_event.set()
