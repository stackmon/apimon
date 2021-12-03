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

import logging

try:
    from alertaclient.api import Client as alerta_client
    from alertaclient import exceptions as alerta_exceptions
except ImportError:
    alerta_client = None


class AlertaClient:
    log = logging.getLogger('apimon.AlertaClient')

    def __init__(self, config):
        self.config = config
        self.retries = 0
        self.client = None

    def connect(self, force=False):
        if alerta_client:
            if force:
                self.retries = 0
            if self.retries > 2:
                self.log.warning(
                    "Reached alerta reconnect limit. Not reconnecting anymore."
                )
                self.client = None
                return
            else:
                self.retries += 1

            alerta_ep = self.config.get_default('alerta', 'endpoint')
            alerta_token = self.config.get_default('alerta', 'token')
            if alerta_ep and alerta_token:
                self.client = alerta_client(
                    endpoint=alerta_ep,
                    token=alerta_token)

    def send_alert(
        self, severity: str, environment: str, resource: str, service: list,
        value: str, origin: str, event: str='Failure', raw_data: str=None
    ) -> None:
        if not self.client:
            self.log.warning('Sending alert is skipped')
            return
        try:
            self.client.send_alert(
                severity=severity,
                environment=environment,
                service=service,
                origin=origin,
                resource=resource,
                event=event,
                value=value,
                raw_data=raw_data
            )
        except alerta_exceptions.AlertaException as ex:
            if ex.message == 'Token is invalid':
                self.connect()
            self.log.error(
                f"Failed to communicate with alerta: {str(ex)}"
            )

    def heartbeat(
        self, origin: str, tags: list, attributes: dict
    ):
        if not self.client:
            return
        try:
            self.client.heartbeat(
                origin=origin,
                tags=tags,
                attributes=attributes
            )
        except alerta_exceptions.AlertaException as ex:
            if ex.message == 'Token is invalid':
                self.connect()
            self.log.error(
                f"Failed to communicate with alerta: {str(ex)}"
            )
