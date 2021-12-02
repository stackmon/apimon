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

from unittest import TestCase
from unittest import mock

from apimon.lib import config


class TestConfig(TestCase):

    def setUp(self):
        self.args = mock.Mock()
        # TODO: generate config

    def test_default(self):
        cfg = config.Config()
        cfg.read('etc/apimon.yaml')

        self.assertEqual('wrk', cfg.get_default('scheduler', 'work_dir'))
        self.assertEqual('bar', cfg.get_default('statsd', 'foo'))
        self.assertEqual('localhost', cfg.get_default('statsd', 'host'))
