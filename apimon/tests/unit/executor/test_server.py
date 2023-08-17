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

import configparser
import mock
import unittest
import uuid
import tempfile
import os
import shutil
import socket
import subprocess
import time

from pathlib import Path

from apimon import project as _project
from apimon.lib import config as _config
from apimon.executor import server
from apimon.executor import message


class TestBase(unittest.TestCase):
    def setUp(self):
        super(TestBase, self).setUp()
        self.config = _config.Config()
        self.config.read('etc/apimon.yaml')
        temp_dir = tempfile.mkdtemp()
        self.work_dir = Path(temp_dir)
        self.project = _project.Project('fake_proj', 'fake_url', 'master',
                                        'ansible', 'fake_loc', 'fake_cmd %s',
                                        self.work_dir)
        os.mkdir(self.project.project_dir)

        self.executor_server = mock.Mock()
        self.executor_server.name = 'executor'
        self.executor_server.hostname = 'test_server'
        self.executor_server.zone = 'fake_zone'
        self.executor_server.config = self.config
        self.executor_server.result_processor = mock.Mock()
        _projects = {
            'fake_proj': self.project
        }
        self.executor_server._projects = _projects
        self.job = mock.Mock()
        self.job.unique = uuid.uuid4().hex
        self.job.sendWorkData = mock.Mock()
        self.job.arguments = (
            '{"env":{"name":"env_name","vars":[]},'
            '"project":{"name":"fake_proj","task":"fake_task"},'
            '"job_id":"fake_job_id"}'
        )
        self.base_job = server.BaseJob(self.executor_server, self.job)
        self.base_job.statsd = mock.Mock()

    def tearDown(self):
        shutil.rmtree(self.work_dir)
        super(TestBase, self).tearDown()


class TestBaseJob(TestBase):

    def test_base(self):
        self.assertIsNotNone(self.base_job.socket_path)
        self.assertDictEqual(
            {
                'zone': self.executor_server.zone,
                'environment': self.base_job.arguments['env']['name']
            },
            self.base_job.statsd_extra_keys
        )
        self.assertEqual('fake_job_id', self.base_job.job_id)
        self.assertIsNotNone(self.base_job.log)

    @mock.patch('subprocess.Popen', autospec=True)
    def test_run(self, sp_mock):
        env_cmp = os.environ.copy()
        env_cmp['TASK_EXECUTOR_JOB_ID'] = self.base_job.job_id
        env_cmp['APIMON_PROFILER_MESSAGE_SOCKET'] = Path(
            self.base_job.job_work_dir, '.comm_socket').resolve().as_posix()

        self.base_job.run()
        self.base_job.wait()
        sp_mock.assert_called_with(
            ['fake_cmd', 'fake_task'],
            stdout=mock.ANY,
            stderr=subprocess.STDOUT,
            preexec_fn=server.preexec_function,
            env=env_cmp,
            cwd=self.base_job.job_work_dir,
            restore_signals=False
        )

    @mock.patch('subprocess.Popen', autospec=True)
    def test_execute(self, sp_mock):
        env_cmp = os.environ.copy()
        env_cmp['TASK_EXECUTOR_JOB_ID'] = self.base_job.job_id
        env_cmp['APIMON_PROFILER_MESSAGE_SOCKET'] = Path(
            self.base_job.job_work_dir, '.comm_socket').resolve().as_posix()

        self.base_job._prepare_local_work_dir()
        log_file = Path(self.work_dir, 'logfile')
        self.base_job._execute(None, log_file)
        sp_mock.assert_called_with(
            ['fake_cmd', 'fake_task'],
            stdout=mock.ANY,
            stderr=subprocess.STDOUT,
            preexec_fn=server.preexec_function,
            env=env_cmp,
            cwd=self.base_job.job_work_dir,
            restore_signals=False
        )

    def test_prepare_local_work_dir(self):
        self.base_job._prepare_local_work_dir()
        self.assertTrue(os.path.exists(self.base_job.job_work_dir))

    def _write_to_socket(self, socket_path, data):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(socket_path)
            msg = '%s\n' % data.serialize()
            s.sendall(msg.encode('utf8'))

    def test_socket(self):
        self.base_job.socket_path = Path(
            self.work_dir, '.comm_socket').as_posix()
        self.base_job.running = True
        self.base_job._setup_communication_socket()
        self.assertTrue(os.path.exists(self.base_job.socket_path))
        metric_c = message.Metric(
            name='metric_c', value=42, metric_type='c')
        metric_ms = message.Metric(
            name='metric_ms', value=43, metric_type='ms')
        metric_ms2 = message.Metric(
            name='metric_ms2', value=43, metric_type='ms', name_suffix='abc')
        metric_g = message.Metric(
            name='metric_g', value=44, metric_type='g')
        result_task = message.ResultTask(
            name='result_task',
            result=0,
            duration=1,
            action='act',
            az='fake_az'
        )
        result_summary = message.ResultSummary(
            name='result_summary',
            result=0,
            duration=1,
        )

        self._write_to_socket(self.base_job.socket_path, metric_c)
        self._write_to_socket(self.base_job.socket_path, metric_ms)
        self._write_to_socket(self.base_job.socket_path, metric_ms2)
        self._write_to_socket(self.base_job.socket_path, metric_g)
        self._write_to_socket(self.base_job.socket_path, result_task)
        self._write_to_socket(self.base_job.socket_path, result_summary)
        # Need to give some time for async processing
        time.sleep(1)
        incr_calls = [
            mock.call(
                'apimon.metric.{environment}.{zone}.%s' % metric_c['name'],
                metric_c['value']),
            mock.call(
                'apimon.metric.{environment}.{zone}.%s.attempted' %
                metric_ms['name'], 1),
            mock.call(
                'apimon.metric.{environment}.{zone}.%s.attempted' %
                metric_ms2['name'], 1),
            mock.call(
                'apimon.metric.{environment}.{zone}.%s.%s' %
                (metric_ms2['name'], metric_ms2['name_suffix']), 1),

        ]
        timing_calls = [
            mock.call(
                'apimon.metric.{environment}.{zone}.%s' % metric_ms['name'],
                metric_ms['value']),
            mock.call(
                'apimon.metric.{environment}.{zone}.%s.%s' %
                (metric_ms2['name'], metric_ms2['name_suffix']),
                metric_ms2['value']),
        ]
        gauge_calls = [
            mock.call(
                'apimon.metric.{environment}.{zone}.%s' % metric_g['name'],
                metric_g['value'])
        ]

        self.base_job.statsd.incr.assert_has_calls(incr_calls)
        self.base_job.statsd.timing.assert_has_calls(timing_calls)
        self.base_job.statsd.gauge.assert_has_calls(gauge_calls)

        additional_attrs = {
            'zone': self.executor_server.zone,
            'environment': 'env_name',
            'job_id': self.base_job.job_id
        }
        result_task.update(additional_attrs)
        result_summary.update(additional_attrs)
        calls = [
            mock.call(result_task),
            mock.call(result_summary)
        ]
        self.executor_server.result_processor.add_entry.assert_has_calls(calls)

        self.base_job.running = False
        self.base_job._teardown_communication_socket()

    def test_base_job_data(self):
        self.assertEqual(
            {
                'worker_name': self.executor_server.name,
                'worker_hostname': self.executor_server.hostname
            },
            self.base_job._base_job_data()
        )


class TestAnsibleJob(TestBase):

    def setUp(self):
        super(TestAnsibleJob, self).setUp()
        self.job = server.AnsibleJob(self.executor_server, self.job)

    def test_basic(self):
        self.assertIsNotNone(self.job.ansible_plugin_path)

    def test_prepare_ansible_cfg(self):
        self.job._prepare_local_work_dir()
        self.job._prepare_ansible_cfg(self.job.job_work_dir)

        config = configparser.ConfigParser()
        ansible_cfg = Path(self.job.job_work_dir, 'ansible.cfg')
        config.read(ansible_cfg)
        self.assertEqual('apimon_profiler',
                         config['defaults']['callback_enabled'])
        self.assertEqual('apimon_logger',
                         config['defaults']['stdout_callback'])
        self.assertEqual(self.job.socket_path,
                         config['callback_apimon_profiler']['socket'])

    @mock.patch(
        'subprocess.Popen', spec=True, stdout='', stderr=mock.MagicMock())
    def test_execute(self, sp_mock):
        env_cmp = os.environ.copy()
        env_cmp['TASK_EXECUTOR_JOB_ID'] = self.job.job_id
        env_cmp['ANSIBLE_CALLBACK_PLUGINS'] = \
            self.job.ansible_plugin_path
        env_cmp['APIMON_EXECUTOR_JOB_CONFIG'] = Path(
            self.job.job_work_dir, 'logging.json').resolve()
        env_cmp['ANSIBLE_PYTHON_INTERPRETER'] = '/usr/bin/python3'
        env_cmp['APIMON_PROFILER_MESSAGE_SOCKET'] = Path(
            self.job.job_work_dir, '.comm_socket').resolve().as_posix()

        env_cmp['STATSD_PREFIX'] = (
            'openstack.api.{environment}.{zone}'
            .format(**self.job.statsd_extra_keys)
        )

        self.job._prepare_local_work_dir()
        log_file = Path(self.job.job_work_dir, 'job-output.txt')
        self.job._execute(self.job.job_work_dir, log_file)
        sp_mock.assert_called_with(
            ['fake_cmd', 'fake_task'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=server.preexec_function,
            env=env_cmp,
            cwd=self.job.job_work_dir,
            restore_signals=False
        )
