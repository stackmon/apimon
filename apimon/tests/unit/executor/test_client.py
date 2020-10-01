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
import datetime
import time
import json

from unittest import mock, TestCase

from apimon import project
from apimon import model
from apimon.executor import client
from apimon.scheduler import scheduler as _scheduler
from apimon.lib import config as _config


class TestScheduleWatcher(TestCase):

    def setUp(self):
        super(TestScheduleWatcher, self).setUp()

    def test_no_jobs(self):
        matrix = model.Matrix()
        executor_client = mock.Mock()
        executor_client._submit_gear_task = mock.Mock()
        watcher = client.ScheduleWatcher(executor_client, matrix)
        watcher.start()

        time.sleep(2)

        executor_client._submit_gear_task.assert_not_called()

        watcher.stop()
        watcher.join()

    def test_jobs(self):
        matrix = model.Matrix()
        executor_client = mock.Mock()
        executor_client._submit_gear_task = mock.Mock()

        def start_processing(task):
            task.scheduled_at = datetime.datetime.now()
        executor_client._submit_gear_task.side_effect = start_processing

        watcher = client.ScheduleWatcher(executor_client, matrix)
        watcher.start()
        inst = model.JobTask(None, None, None)
        matrix.send_neo('prj', 'task', 'env', inst)

        time.sleep(3)

        executor_client._submit_gear_task.assert_called_once_with(inst)

        watcher.stop()
        watcher.join()

    def test_jobs_processed(self):
        matrix = model.Matrix()
        executor_client = mock.Mock()
        executor_client._submit_gear_task = mock.Mock()
        executor_client._submit_gear_task.side_effect = lambda x: x.reset_job()
        watcher = client.ScheduleWatcher(executor_client, matrix)
        watcher.start()
        inst = model.JobTask(None, None, None)
        matrix.send_neo('prj', 'task', 'env', inst)

        time.sleep(4)

        executor_client._submit_gear_task.assert_any_call(inst)

        watcher.stop()
        watcher.join()

    def test_jobs_pause(self):
        matrix = model.Matrix()
        executor_client = mock.Mock()
        executor_client._submit_gear_task = mock.Mock()
        executor_client._submit_gear_task.side_effect = lambda x: x.reset_job()
        watcher = client.ScheduleWatcher(executor_client, matrix)
        watcher.start()
        watcher.pause()
        inst = model.JobTask(None, None, None)
        matrix.send_neo('prj', 'task', 'env', inst)

        time.sleep(2)

        executor_client._submit_gear_task.assert_not_called()

        watcher.stop()
        watcher.join()


class TestJobExecutorClient(TestCase):

    def setUp(self):
        super(TestJobExecutorClient, self).setUp()

        self.config = _config.Config()
        self.config.read('etc/apimon.yaml')
        self.config.config['test_matrix'] = [
            {'project': 'ansible_project', 'env': 'fake_env'},
            {
                'project': 'misc_project',
                'env': 'fake_env',
                'tasks': [
                    'sc1',
                    {'sc2': {'interval': 50}}
                ]}
        ]
        self.scheduler = _scheduler.Scheduler(self.config)
        self.scheduler._projects = {
            'ansible_project': project.Project('ansible_project', 'fake_url'),
            'misc_project': project.Project(
                'misc_project', 'fake_url', type='misc',
                location='loc')
        }
        self.scheduler._projects['ansible_project']._tasks = ['a1']
        self.scheduler._environments = {
            'fake_env': model.TestEnvironment(None, None, 'fake_env')
        }

    def test_basic(self):
        cl = client.JobExecutorClient(self.config, None)
        cl.start()

        cl.stop()

    @mock.patch('apimon.project.Project.get_commit',
                return_value='fake_commit')
    @mock.patch('apimon.executor.client.ApimonGearClient')
    def test_1(self, gear_mock, git_mock):
        gear_mock.submitJob = mock.Mock()

        cl = client.JobExecutorClient(self.config, self.scheduler)
        cl.start()
        cl.resume_scheduling()

        time.sleep(5)

        mock_calls = cl.gearman.submitJob.mock_calls

        # verify interval set properly
        self.assertEqual(
            50, cl._matrix._matrix['misc_project']['sc2']['fake_env'].interval)
        self.assertEqual(
            0, cl._matrix._matrix['misc_project']['sc1']['fake_env'].interval)

        found = 0
        for call in mock_calls:
            # Count jobs that we would expect
            job = call.args[0]  # job argument
            args = json.loads(job.arguments)
            print(job)
            prj = args['project']
            if prj['name'] == 'ansible_project':
                if prj['task'] in ['a1']:
                    found += 1
            elif prj['name'] == 'misc_project':
                if prj['task'] in ['loc/sc1', 'loc/sc2']:
                    found += 1
        self.assertEqual(3, found)

        cl.stop()
