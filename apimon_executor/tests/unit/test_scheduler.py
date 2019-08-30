
import os
import copy
from pathlib import Path
import configparser
import argparse
import tempfile
from unittest import TestCase
from unittest import mock, skip
from concurrent.futures import ThreadPoolExecutor
import time
from apimon_executor import scheduler as _scheduler
from apimon_executor import config as _config
import subprocess
from random import randrange
import yaml

_timeout = time.time() + 10


def _set_config():
    config_dict = {
        'executor': {
            'repo': 'https://github.com/opentelekomcloud-infra/api-monitoring',
            'git_checkout_dir': 'wrk',
            'git_ref': 'master',
            'git_refresh_interval': 2,
            'location': 'playbooks/scenarios',
            'count_executor_threads': 8
        }
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        config_file = Path(tmpdir, 'config.yaml')
        config_file.touch()
        with open(config_file, 'w') as yaml_file:
            yaml.dump(config_dict, yaml_file, default_flow_style=False)
        args = argparse.Namespace()
        args.config = config_file
        return _config.ExecutorConfig(args)


class ExecutorTest(TestCase):
    def setUp(self):
        self._cnf = _set_config()
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
        command = "time.sleep {}".format(randrange(5, 9))
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            shell=True,
            stderr=subprocess.STDOUT)
        process.wait()
        if time.time() > _timeout:
            self.shutdown_event.set()

    @skip('')
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

            scheduler = _scheduler.Scheduler(
                self.task_queue,
                self.finished_task_queue,
                self.shutdown_event,
                self.ignore_tasks_event,
                cnf)
            with ThreadPoolExecutor(max_workers=(
                cnf.count_executor_threads + 1)) as thread_pool:
                thread_pool.submit(scheduler.run)
                for i in range(cnf.count_executor_threads):
                    executor = _scheduler.Executor(
                        self.task_queue,
                        self.finished_task_queue,
                        self.shutdown_event,
                        self.ignore_tasks_event,
                        cnf)
                    thread_pool.submit(executor.run)


class SchedulerTest(TestCase):
    def setUp(self):
        self._cnf = _set_config()
        ApimonScheduler = _scheduler.ApimonScheduler(None)
        self._shutdown_event = ApimonScheduler.shutdown_event
        self._ignore_tasks_event = ApimonScheduler.ignore_tasks_event
        self._task_queue = ApimonScheduler.task_queue
        self._finished_task_queue = ApimonScheduler.finished_task_queue

    def test_schedule_tasks(self):
        cnf = self._cnf
        scenarios = []
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(Path(tmpdir, 'dummy'))
            for i in range(10):
                Path(tmpdir, 'dummy/scenario{}.yaml'.format(i)).touch()
                scenarios.append('scenario{}.yaml'.format(i))
            setattr(cnf, 'scenarios', scenarios)
            setattr(cnf, 'location', 'dummy')
            setattr(cnf, 'git_checkout_dir', tmpdir)
            setattr(cnf, 'log_dest', tmpdir)
            scheduler = _scheduler.Scheduler(
                self._task_queue, None, None, None, cnf)
            with mock.patch.object(scheduler, 'schedule_tasks',
                                   wraps=scheduler.schedule_tasks
                                   ) as mock_schedule_tasks:
                scheduler.schedule_tasks(scheduler.task_queue)
                self.assertTrue(mock_schedule_tasks.called)
                self.assertEqual(scheduler.task_queue.qsize(), len(scenarios))

    def test_discard_queue_tasks(self):
        task_queue = self._task_queue
        task_queue.put('task1')
        task_queue.put('task1')
        scheduler = _scheduler.Scheduler(task_queue, None, None, None)
        self.assertTrue(scheduler.task_queue.qsize() != 0)
        scheduler.discard_queue_elements(scheduler.task_queue)
        self.assertTrue(scheduler.task_queue.qsize() == 0)

    @mock.patch(
        "apimon_executor.scheduler.Scheduler.get_git_repo",
        mock.MagicMock(
            return_value="test_repo"))
    @mock.patch(
        "apimon_executor.scheduler.Scheduler.refresh_git_repo")
    @mock.patch(
        "apimon_executor.scheduler.Scheduler.is_repo_update_necessary")
    def test_refresh_git_repo(
        self,
        mock_refresh_repo,
        mock_check_repo_update):

        cnf = self._cnf
        scenarios = []
        with tempfile.TemporaryDirectory() as tmpdir:
            os.mkdir(Path(tmpdir, 'dummy'))
            for i in range(10):
                Path(tmpdir, 'dummy/scenario{}.yaml'.format(i)).touch()
                scenarios.append('scenario{}.yaml'.format(i))
            setattr(cnf, 'scenarios', scenarios)
            setattr(cnf, 'location', 'dummy')
            setattr(cnf, 'git_checkout_dir', tmpdir)
            scheduler = _scheduler.Scheduler(
                self._task_queue,
                self._finished_task_queue,
                self._shutdown_event,
                self._ignore_tasks_event,
                cnf)
            with ThreadPoolExecutor(max_workers=1) as thread_pool:
                with mock.patch.object(scheduler, 'schedule_tasks',
                                       wraps=scheduler.schedule_tasks
                                       ) as mock_schedule_tasks:
                    mock_check_repo_update.return_value = False
                    thread_pool.submit(scheduler.run)
                    time.sleep(0.2)
                    self.assertTrue(mock_schedule_tasks.called)

                    self.assertFalse(scheduler.ignore_tasks_event.is_set())
                    self.assertFalse(scheduler.shutdown_event.is_set())

                    self.assertFalse(mock_refresh_repo.called)
                    self.assertEqual(
                        scheduler.task_queue.qsize(), len(scenarios))

                    task_item = scheduler.task_queue.get(False, 1)
                    scheduler.finished_task_queue.put(task_item)
                    scheduler.task_queue.task_done()
                    self.assertEqual(
                        scheduler.task_queue.qsize(),
                        len(scenarios) - 1)
                    time.sleep(1)
                    self.assertEqual(
                        scheduler.task_queue.qsize(), len(scenarios))
                    self.assertEqual(scheduler.finished_task_queue.qsize(), 0)

                    self.assertEqual(mock_schedule_tasks.call_count, 1)
                    mock_check_repo_update.return_value = True
                    time.sleep(cnf.git_refresh_interval)
                    self.assertEqual(mock_schedule_tasks.call_count, 2)
                    mock_check_repo_update.return_value = False
                    self.assertTrue(mock_refresh_repo.called)
                    scheduler.shutdown_event.set()

    @mock.patch(
        "apimon_executor.scheduler.Scheduler.refresh_git_repo")
    def test_get_git_repo(self, mock_refresh_repo):
        cnf = self._cnf
        scheduler = _scheduler.Scheduler(None, None, None, None)
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_dir = Path(tmpdir, cnf.git_checkout_dir)
            os.mkdir(repo_dir)

            self.assertFalse(Path(repo_dir, '.git').exists())
            scheduler.get_git_repo(cnf.repo, repo_dir, cnf.git_ref)
            self.assertFalse(mock_refresh_repo.called)
            self.assertTrue(Path(repo_dir, '.git').exists())

            # Call get_git_repo again when repo already exists
            scheduler.get_git_repo(cnf.repo, repo_dir, cnf.git_ref)
            self.assertTrue(mock_refresh_repo.called)
