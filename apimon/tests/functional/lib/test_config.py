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

from apimon.lib import config


class TestConfig(TestCase):
    def setUp(self):
        self.vault_client = hvac.Client(
            url='http://localhost:8200',
            token='root'
        )

    def test_get_cloud_with_vault(self):
        cfg = config.Config()
        cfg._connect_to_vault('http://localhost:8200', token='root')
        vault_connected = False
        try:
            vault_connected = cfg.vault_client.is_authenticated()
        except Exception:
            pass
        finally:
            if not vault_connected:
                self.skipTest('Vault not available for test')

        self.vault_client.secrets.kv.v2.create_or_update_secret(
            path='usr1',
            secret=dict(
                username='foo',
                password='bar'
            )
        )
        cfg.config = dict(clouds=[])
        cfg.config['clouds'] = [
            dict(
                name='vault_cloud',
                data=dict(
                    auth=dict(
                        username="vault|engine:secret|path:usr1|attr:username",
                        password="vault|engine:secret|path:usr1|attr:password"
                    )
                )
            )
        ]

        cloud = cfg.get_cloud('clouds', 'vault_cloud')
        self.assertDictEqual({
            'auth': {
                'username': 'foo',
                'password': 'bar'
            },
        }, cloud)
