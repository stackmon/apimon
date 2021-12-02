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


import hvac

from apimon.lib import utils


class TestUtils(TestCase):

    def test_expand_vars(self):
        self.assertEqual(
            'abc',
            utils.expand_vars('abc')
        )

    def test_expand_vars_no_vault(self):
        self.assertEqual(
            'vault|fake',
            utils.expand_vars('vault|fake')
        )

    def test_expand_vars_vault(self):
        with mock.patch(
            'hvac.api.secrets_engines.kv_v2.KvV2.read_secret'
        ) as mock_vault:
            mock_vault.return_value = {'data': {'data': {'attr_name': 'bar'}}}

            fake_client = hvac.Client(
                url='http://fake.com:8200',
                token='fake_token',
                timeout=1
            )
            self.assertEqual(
                'bar',
                utils.expand_vars(
                    'vault|engine:secret|path:fake_path|attr:attr_name',
                    fake_client)
            )
