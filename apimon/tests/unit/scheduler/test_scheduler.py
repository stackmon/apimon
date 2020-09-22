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
import time

from unittest import mock, TestCase

from apimon.scheduler import scheduler as _scheduler
from apimon.lib import config as _config


class TestProjectCleanup(TestCase):

    def setUp(self):
        super(TestProjectCleanup, self).setUp()

        self.config = _config.Config()
        self.config.read('etc/apimon.yaml')

    @mock.patch('openstack.config')
    @mock.patch('openstack.connection')
    def test_basic(self, sdk_mock, config_mock):
        conn_mock = mock.Mock()
        sdk_mock.Connection = mock.Mock(return_value=conn_mock)
        conn_mock.project_cleanup = mock.Mock()

        pc = _scheduler.ProjectCleanup(self.config)
        pc.start()
        pc.wake_event.set()

        time.sleep(2)

        conn_mock.project_cleanup.assert_called_with(
            wait_timeout=600, filters={
                'created_at': mock.ANY})

        pc.stop()
        pc.join()


class TestScheduler(TestCase):

    def setUp(self):
        super(TestScheduler, self).setUp()

        self.config = _config.Config()
        self.config.read('etc/apimon.yaml')

        self.scheduler = _scheduler.Scheduler(self.config)

    def test_basic(self):
        self.assertTrue(True)
        #self.scheduler.start()

        #self.scheduler.stop()
