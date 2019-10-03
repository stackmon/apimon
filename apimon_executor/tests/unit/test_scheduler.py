
import os
from pathlib import Path
import configparser
import argparse
import tempfile
import threading
from unittest import TestCase, mock
from mock import call
import time
import shutil
import uuid
import yaml

from apimon_executor import scheduler as _scheduler
from apimon_executor import config as _config
from apimon_executor import queue as _queue


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


class TestTaskItem(TestCase):

    def setUp(self):
        super(TestTaskItem, self).setUp()
        self.prj = mock.Mock()
        self.prj.name = 'fake_project'
        self.item = _scheduler.TaskItem(self.prj, 'fake_item')

    def test_default(self):
        self.assertEqual(self.prj, self.item.project)
        self.assertEqual('fake_item', self.item.item)
        self.assertEqual('fake_project.fake_item', self.item.name)

    def test_get_exec_cmd(self):
        self.item.get_exec_cmd()
        self.prj.get_exec_cmd.assert_called_with(self.item.item)

    def test_is_task_valid(self):
        self.item.is_task_valid()
        self.prj.is_task_valid.assert_called_with(self.item.item)


class TestApimonScheduler(TestCase):

    def setUp(self):
        super(TestApimonScheduler, self).setUp()
        self.config = mock.Mock()

    def test_init(self):
        scheduler = _scheduler.ApimonScheduler(self.config)
        self.assertEqual(self.config, scheduler.config)

        self.assertTrue(isinstance(scheduler.task_queue, _queue.UniqueQueue))
        self.assertTrue(isinstance(
            scheduler.finished_task_queue, _queue.UniqueQueue))

    def test_signal_handler(self):
        scheduler = _scheduler.ApimonScheduler(self.config)
        with self.assertLogs('apimon_executor', level='INFO') as cm:
            scheduler.signal_handler(1, 1)
            self.assertTrue(scheduler.shutdown_event.is_set())
            self.assertIn(
                'INFO:apimon_executor:Signal received. '
                'Gracefully stop processing',
                cm.output)

    def test_start(self):
        # This is a big TODO
        pass


class TestScheduler(TestCase):

    def setUp(self):
        super(TestScheduler, self).setUp()
        self.config = mock.Mock()
        self.config.fake1k = 'fake1v'
        self.config.projects = {
            'p1': mock.MagicMock(),
            'p2': mock.MagicMock()
        }
        self.task_queue = mock.Mock()
        self.finished_task_queue = mock.Mock()
        self.shutdown_event = mock.Mock()
        self.pause_event = mock.Mock()

        self.scheduler = _scheduler.Scheduler(
            self.task_queue,
            self.finished_task_queue,
            self.shutdown_event,
            self.pause_event,
            self.config)

    def test_init(self):
        self.assertEqual(self.task_queue, self.scheduler.task_queue)
        self.assertEqual(self.finished_task_queue,
                         self.scheduler.finished_task_queue)
        self.assertEqual(self.shutdown_event, self.scheduler.shutdown_event)
        self.assertEqual(self.pause_event, self.scheduler.pause_event)
        self.assertEqual('fake1v', self.scheduler._fake1k)
        self.assertEqual(self.config.projects, self.scheduler._projects)

    def test_init_projects(self):
        self.scheduler._init_projects()
        for n, p in self.scheduler._projects.items():
            p.prepare.assert_called()

    def test_refresh_projects(self):
        self.scheduler._refresh_interval = 120
        self.scheduler.set_next_refresh_time(1)
        self.scheduler.schedule_tasks = mock.Mock()
        now = time.time()
        self.scheduler.refresh_projects()

        calls = []

        for n, p in self.scheduler._projects.items():
            p.is_repo_update_necessary.assert_called()
            p.refresh_git_repo.assert_called()
            calls.append(call(self.scheduler.task_queue, p))

        self.scheduler.schedule_tasks.assert_has_calls(calls)
        self.assertTrue(self.scheduler.get_next_refresh_time() >= now + 120)

    def test_schedule_tasks(self):
        p1 = mock.Mock()
        p1.name = 'p1'
        p1.tasks.return_value = ['p1_t1', 'p1_t2']
        p2 = mock.Mock()
        p2.name = 'p2'
        p2.tasks.return_value = ['p2_t1', 'p2_t2']
        queue = _queue.UniqueQueue()

        self.scheduler.schedule_tasks(queue, p1)
        self.scheduler.schedule_tasks(queue, p2)

        scheduled_items = set()

        while not queue.empty():
            scheduled_items.add(queue.get().name)

        self.assertEqual({'p2.p2_t2', 'p1.p1_t1', 'p1.p1_t2', 'p2.p2_t1'},
                         scheduled_items)

    def test_run(self):
        p1 = mock.Mock()
        p1.name = 'p1'
        p1.tasks.return_value = ['p1_t1', 'p1_t2']
        p1.prepare = mock.Mock()
        p2 = mock.Mock()
        p2.name = 'p2'
        p2.prepare = mock.Mock()
        p2.tasks.return_value = ['p2_t1', 'p2_t2']
        self.scheduler._projects = {'p1': p1, 'p2': p2}
        self.scheduler.task_queue = _queue.UniqueQueue()
        self.scheduler.finished_task_queue = _queue.UniqueQueue()
        self.scheduler._refresh_interval = 120
        self.scheduler.shutdown_event = threading.Event()
        self.scheduler.sleep_time = 0.1

        try:

            thread = threading.Thread(target=self.scheduler.run)
            thread.start()
            time.sleep(1)

            scheduled_items = set()

            while not self.scheduler.task_queue.empty():
                scheduled_items.add(self.scheduler.task_queue.get().name)

            self.assertEqual({'p2.p2_t2', 'p1.p1_t1', 'p1.p1_t2', 'p2.p2_t1'},
                             scheduled_items)

            self.scheduler.finished_task_queue.put(
                _scheduler.TaskItem(p1, 'p1_t3'))

            # Wait longer than default sleep in thread
            time.sleep(1)

            scheduled_items.clear()

            while not self.scheduler.task_queue.empty():
                scheduled_items.add(self.scheduler.task_queue.get().name)

            self.assertIn('p1.p1_t3', scheduled_items)

        finally:
            self.scheduler.shutdown_event.set()

            thread.join()


class TestExecutor(TestCase):

    def setUp(self):
        super(TestExecutor, self).setUp()
        self.config = mock.Mock()
        self.config.fake1k = 'fake1v'
        self.config.log_dest = tempfile.TemporaryDirectory().name
        self.config.projects = {
            'p1': mock.MagicMock(),
            'p2': mock.MagicMock()
        }
        self.task_queue = mock.Mock()
        self.finished_task_queue = mock.Mock()
        self.shutdown_event = mock.Mock()
        self.pause_event = mock.Mock()

        self.executor = _scheduler.Executor(
            self.task_queue,
            self.finished_task_queue,
            self.shutdown_event,
            self.pause_event,
            self.config)

    def test_init(self):
        self.assertEqual(self.task_queue, self.executor.task_queue)
        self.assertEqual(self.finished_task_queue,
                         self.executor.finished_task_queue)
        self.assertEqual(self.shutdown_event, self.executor.shutdown_event)
        self.assertEqual(self.pause_event, self.executor.pause_event)
        self.assertEqual('fake1v', self.executor._fake1k)
        self.assertEqual(self.config.projects, self.executor._projects)

    def test_prepare_ansible_cfg(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # (1) - config was not present
            self.executor._prepare_ansible_cfg(tmp_dir)

            ansible_cfg = Path(tmp_dir, 'ansible.cfg')
            self.assertTrue(ansible_cfg.exists())

            config = configparser.ConfigParser()
            config.read(ansible_cfg.as_posix())
            self.assertEqual(
                'apimon_logger',
                config['defaults']['stdout_callback'])

            # (2) config contained other value
            config['defaults']['stdout_callback'] = 'fake'
            config['defaults']['dummy'] = 'fake'
            with open(ansible_cfg, 'w') as f:
                config.write(f)

            self.executor._prepare_ansible_cfg(tmp_dir)

            config.read(ansible_cfg.as_posix())

            self.assertEqual({
                'stdout_callback': 'apimon_logger',
                'dummy': 'fake'},
                config['defaults'])

    def test_archive_log(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            log = Path(tmp_dir, 'fake.log')
            open(log.as_posix(), 'w').close()
            self.executor.archive_log_file(log)
            self.assertFalse(log.exists())
            self.assertTrue(log.with_suffix('.txt.gz').exists)

    def test_execute(self):
        with mock.patch('subprocess.Popen') as process_mock, \
                tempfile.TemporaryDirectory() as prj_dir:
            p1 = mock.Mock()
            p1.name = 'p1'
            p1.project_dir = prj_dir
            p1.get_exec_cmd.return_value = 'fake_cmd fake_arg1 fake_arg2'
            job_id = uuid.uuid4().hex
            prepare_mock = mock.Mock()
            self.executor._prepare_ansible_cfg = prepare_mock
            self.executor.archive_log_file = mock.Mock()
            self.executor.execute(_scheduler.TaskItem(p1, 'p1_t1'),
                                  job_id)

            self.assertTrue(Path(self.config.log_dest).exists())
            job_log_dir = Path(self.config.log_dest, str(job_id[-2:]),
                               job_id)
            self.assertTrue(job_log_dir.exists())

            prepare_mock.assert_called()
            prepare_call_args, prepare_call_kwargs = prepare_mock.call_args
            job_work_dir = Path(prepare_call_args[0])

            env = os.environ.copy()
            env['TASK_EXECUTOR_JOB_ID'] = job_id
            env['ANSIBLE_CALLBACK_PLUGINS'] = self.executor.ansible_plugin_path
            env['APIMON_EXECUTOR_JOB_CONFIG'] = Path(job_work_dir,
                                                     'logging.json').as_posix()

            # Remove dir with log output
            shutil.rmtree(job_log_dir.as_posix())
            process_mock.assert_called_with(
                ['fake_cmd', 'fake_arg1', 'fake_arg2'],
                cwd=job_work_dir,
                restore_signals=False,
                stdout=-1, stderr=-1,
                env=env,
                preexec_fn=_scheduler.preexec_function
            )

            self.executor.archive_log_file.assert_called_with(
                Path(job_log_dir, 'job-output.txt'))

    def test_run(self):
        p1 = mock.Mock()
        p1.name = 'p1'
        p1.tasks.return_value = ['p1_t1', 'p1_t2']
        p1.prepare = mock.Mock()
        p2 = mock.Mock()
        p2.name = 'p2'
        p2.prepare = mock.Mock()
        p2.tasks.return_value = ['p2_t1', 'p2_t2']
        self.executor.task_queue = _queue.UniqueQueue()
        self.executor.finished_task_queue = _queue.UniqueQueue()
        self.executor.shutdown_event = threading.Event()
        self.executor.pause_event = threading.Event()
        self.executor.execute = mock.Mock(
            side_effect=[True, Exception('boom')])
        self.executor._simulate = False
        self.executor.sleep_time = 0.1

        try:

            task1 = _scheduler.TaskItem(p1, 'p1_t1')
            task2 = _scheduler.TaskItem(p2, 'p2_t1')

            self.executor.task_queue.put(task1)
            self.executor.task_queue.put(task2)

            thread = threading.Thread(target=self.executor.run)
            thread.start()

            rescheduled_items = set()

            calls = [
                mock.call(task1, mock.ANY),
                mock.call(task2, mock.ANY)
            ]

            time.sleep(1)
            self.executor.execute.assert_has_calls(calls)

            while not self.executor.finished_task_queue.empty():
                rescheduled_items.add(
                    self.executor.finished_task_queue.get().name)

            self.assertIn('p1.p1_t1', rescheduled_items)
            self.assertIn('p2.p2_t1', rescheduled_items)

        finally:
            self.executor.shutdown_event.set()
            thread.join()
