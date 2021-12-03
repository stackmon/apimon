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

import requests_mock

from apimon.lib import alerta_client
from apimon.lib import config


class TestAlertaClient(TestCase):
    def setUp(self):
        cfg = config.Config()
        cfg.config['alerta'] = dict(
            endpoint='http://alerta',
            token='foo'
        )
        self.alerta_client = alerta_client.AlertaClient(cfg)
        self.adapter = requests_mock.Adapter()

    def test_connect(self):
        self.alerta_client.connect()

    def test_alert(self):
        self.alerta_client.connect()
        with requests_mock.Mocker() as rmock:
            rmock.post('http://alerta/alert', json={})
            self.alerta_client.send_alert(
                severity='sev',
                environment='env',
                resource='res',
                service='srv',
                value='val',
                origin='origin'
            )
            self.assertEqual(1, len(rmock.request_history))

    def test_alert_skip(self):
        with requests_mock.Mocker() as rmock:
            rmock.post('http://alerta/alert', json={})
            self.alerta_client.send_alert(
                severity='sev',
                environment='env',
                resource='res',
                service='srv',
                value='val',
                origin='origin'
            )
            self.assertEqual(0, len(rmock.request_history))

    def test_heartbeat(self):
        self.alerta_client.connect()
        with requests_mock.Mocker() as rmock:
            rmock.post(
                'http://alerta/heartbeat',
                json={'heartbeat': {}})
            self.alerta_client.heartbeat(
                origin='origin',
                tags=[],
                attributes={}
            )
            self.assertEqual(1, len(rmock.request_history))

    def test_heartbeat_skip(self):
        with requests_mock.Mocker() as rmock:
            rmock.post(
                'http://alerta/heartbeat',
                json={'heartbeat': {}})
            self.alerta_client.heartbeat(
                origin='origin',
                tags=[],
                attributes={}
            )
            self.assertEqual(0, len(rmock.request_history))
