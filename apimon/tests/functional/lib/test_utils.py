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

import hvac

from apimon.lib import utils


class TestUtils(TestCase):
    def test_expand_vars_vault(self):
        vault_client = hvac.Client(
            url='http://localhost:8200',
            token='root'
        )
        vault_client.secrets.kv.v2.create_or_update_secret(
            path='usr1',
            secret=dict(
                username='foo',
                password='bar'
            )
        )
        self.assertEqual(
            'bar',
            utils.expand_vars(
                'vault|engine=secret|path=usr1|attr=password',
                vault_client)
        )

    def test_expand_dict(self):
        vault_client = hvac.Client(
            url='http://localhost:8200',
            token='root'
        )
        vault_client.secrets.kv.v2.create_or_update_secret(
            path='fake',
            secret=dict(
                foo='bar',
            )
        )
        vault_client.secrets.kv.v2.create_or_update_secret(
            path='fake2',
            secret=dict(
                foo='bar2',
            )
        )
        struct = {
            'foo': 'vault|engine=secret|path=fake|attr=foo',
            'inline': {
                'foo2': 'vault|engine=secret|path=fake2|attr=foo'
            }
        }

        self.assertDictEqual(
            {'foo': 'bar', 'inline': {'foo2': 'bar2'}},
            utils.expand_dict_vars(
                struct,
                vault_client)
        )
