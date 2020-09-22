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
import json
import uuid

from unittest import mock, TestCase

from apimon import project
from apimon import model


class TestJobTask(TestCase):

    def setUp(self):
        super(TestJobTask, self).setUp()
        self.project = project.Project('fake_proj', 'fake_url', 'master',
                                       'ansible', 'fake_loc', 'fake_cmd %s',
                                       'wrk_dir')

        self.project_misc = project.Project('fake_proj', 'fake_url', 'master',
                                            'misc', 'fake_loc', 'misc_cmd %s',
                                            'wrk_dir')
        self.task = uuid.uuid4().hex
        self.env = model.TestEnvironment(None, None, 'fake_env')

    def test_basic(self):
        task = model.JobTask(self.project, self.task, self.env)
        self.assertEqual(self.project, task.project)
        self.assertEqual(self.task, task.task)
        self.assertEqual(self.env, task.env)
        self.assertEqual(datetime.timedelta(minutes=0), task.interval_dt)
        self.assertEqual(0, task.interval)

    @mock.patch('apimon.project.Project.get_commit',
                return_value='fake_commit')
    def test_prepare_gear_job(self, commit_mock):
        task = model.JobTask(self.project, self.task, self.env)
        job = task.prepare_gear_job(None, 5, 'fake_job_id')
        self.assertIsNotNone(task._gear_job_id)
        self.assertEqual('fake_job_id', task._apimon_job_id)
        self.assertIsNotNone(job)
        self.assertEqual('apimon:ansible', job.name)
        self.assertEqual(task._gear_job_id, job.unique)
        self.assertDictEqual({
            'config_version': 5,
            'env': {'name': 'fake_env'},
            'job_id': 'fake_job_id',
            'project': {'commit': 'fake_commit',
                        'exec_cmd': 'fake_cmd %s',
                        'name': 'fake_proj',
                        'ref': 'master',
                        'task': self.task,
                        'url': 'fake_url'}},
            json.loads(job.arguments))

        task.reset_job()

        self.assertIsNone(task._apimon_job_id)
        self.assertIsNone(task._gear_job_id)

    @mock.patch('apimon.project.Project.get_commit',
                return_value='fake_commit')
    def test_prepare_misc_job(self, commit_mock):
        task = model.JobTask(self.project_misc, self.task, self.env)
        job = task.prepare_gear_job(None, 5, 'fake_job_id')
        self.assertIsNotNone(task._gear_job_id)
        self.assertEqual('fake_job_id', task._apimon_job_id)
        self.assertIsNotNone(job)
        self.assertEqual('apimon:misc', job.name)
        self.assertEqual(task._gear_job_id, job.unique)
        self.assertDictEqual({
            'config_version': 5,
            'env': {'name': 'fake_env'},
            'job_id': 'fake_job_id',
            'project': {'commit': 'fake_commit',
                        'exec_cmd': 'misc_cmd %s',
                        'name': 'fake_proj',
                        'ref': 'master',
                        'task': self.task,
                        'url': 'fake_url'}},
            json.loads(job.arguments))


class TestMatrix(TestCase):

    def setUp(self):
        super(TestMatrix, self).setUp()

    def test_repr(self):
        matrix = model.Matrix()
        self.assertIsNotNone(matrix.__repr__())

    def test_send_neo(self):
        matrix = model.Matrix()
        inst = model.JobTask(None, None, None)
        matrix.send_neo('prj', 'task', 'env', inst)
        self.assertDictEqual(
            {'prj': {'task': {'env': inst}}},
            matrix._matrix)

    def test_tasks(self):
        matrix = model.Matrix()
        self.assertEqual([], list(matrix.tasks()))

        inst = model.JobTask(None, None, None)
        matrix.send_neo('prj', 'task', 'env', inst)

        items = list(matrix.tasks())
        self.assertEqual(1, len(items))
        self.assertEqual(inst, items[0])
