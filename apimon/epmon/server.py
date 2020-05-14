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
import logging
import threading
import time

import openstack

try:
    from alertaclient.api import Client as alerta_client
except ImportError:
    alerta_client = None

import influxdb

from apimon.lib import commandsocket


COMMANDS = ['stop', 'pause', 'resume']


class EndpointMonitor(threading.Thread):
    """A thread that checks endpoints. """
    log = logging.getLogger("apimon.EndpointMonitor")

    def __init__(self, config, target_cloud, alerta):
        threading.Thread.__init__(self)
        self.daemon = True
        self.wake_event = threading.Event()
        self._stopped = False
        self._pause = False
        self.config = config
        self.target_cloud = target_cloud
        self.alerta = alerta
        auth_part = None
        self.conn = None

        self.influx_cnf = self.config.get_default('metrics', 'influxdb').copy()

        for cnf in self.config.config.get('clouds', []):
            if cnf.get('name') == target_cloud:
                auth_part = cnf.get('data')
                if 'additional_metric_tags' in auth_part:
                    self.influx_cnf['additional_metric_tags'] = \
                        auth_part['additional_metric_tags']

        if not auth_part:
            raise RuntimeError('Requested cloud %s is not found' %
                               target_cloud)

        override_measurement = self.config.get_default('epmon', 'measurement')
        if override_measurement and self.influx_cnf:
            self.influx_cnf['measurement'] = override_measurement

        self.region = openstack.config.get_cloud_region(
            load_yaml_config=False,
            **auth_part)
        self.region._influxdb_config = self.influx_cnf

    def stop(self) -> None:
        self._stopped = True
        self.wake_event.set()

    def pause(self) -> None:
        self._pause = True

    def resume(self) -> None:
        self._pause = False

    def _connect_influx(self) -> None:
        try:
            self.influxdb_client = influxdb.InfluxDBClient(
                self.influx_cnf['host'],
                self.influx_cnf['port'],
                self.influx_cnf['username'],
                self.influx_cnf['password']
            )
        except Exception:
            self.log.exception('Failed to establish connection to influxDB')

    def run(self) -> None:
        self._connect()
        while True:
            if self._stopped:
                return
            if not self.conn:
                # Not sure whether it works if we loose connection
                self._connect()
            try:
                if self._pause:
                    # Do not send heartbeat as well to not to forget to resume
                    continue
                if self.conn:
                    self._execute()
                if self.alerta:
                    try:
                        self.alerta.heartbeat(
                            origin='apimon.epmon.%s' % self.target_cloud,
                            tags=['apimon', 'epmon']
                        )
                    except Exception:
                        self.log.exception('Error sending heartbeat')

                time.sleep(5)
            except Exception:
                self.log.exception("Exception checking endpoints:")

    def _connect(self):
        self._connect_influx()
        try:
            self.conn = openstack.connection.Connection(
                config=self.region,
            )
        except AttributeError as e:
            # NOTE(gtema): SDK chains attribute error when calling
            # conn.authorize, but response is not present
            self.log.error('Cannot establish connection: %s' % e.__context__)
            self.send_alert('identity', e.__context__)
        except Exception as e:
            self.log.exception('Cannot establish connection to cloud %s: %s' %
                               (self.target_cloud, e))
            self.send_alert('identity', e)

    def _execute(self):
        eps = self.conn.config.get_service_catalog().get_endpoints().items()
        for service, data in eps:
            endpoint = data[0]['url']
            response = None
            error = None
            try:
                client = self.conn.config.get_session_client(service)
                response = client.get(
                    endpoint,
                    headers={'content-type': 'application/json'},
                    timeout=5)
            except Exception as e:
                error = e
                self.log.debug('Got exception for endpoint %s: %s' % (endpoint,
                                                                      e))
            status_code = 0
            if response is not None:
                status_code = int(response.status_code)
            elif error:
                tags = dict(
                    method='GET',
                    service_type=service,
                    status_code=str(status_code),
                    name='discovery')

                if 'additional_metric_tags' in self.influx_cnf:
                    tags.update(self.influx_cnf['additional_metric_tags'])
                data = [dict(
                    measurement=(self.influx_cnf.get('measurement',
                                                     'epmon')),
                    tags=tags,
                    fields=dict(
                        duration=0,
                        status_code_val=int(status_code)
                    )
                )]
                try:
                    self.influxdb_client.write_points(data)
                except Exception:
                    self.log.exception('Error writing statistics to InfluxDB')

            if error or status_code >= 500:
                pass
#                self.send_alert(
#                    resource=service,
#                    value='curl -g -i -X GET %s -H '
#                          '"X-Auth-Token: ${TOKEN}" '
#                          '-H "content-type: application/json" fails' %
#                          endpoint,
#                    raw_data=str(error.message if error else response)
#                )

    def send_alert(self, resource: str, value: str,
                   raw_data: str=None) -> None:
        if self.alerta:
            self.alerta.send_alert(
                severity='critical',
                environment=self.target_cloud,
                service=['apimon', 'endpoint_monitor'],
                resource=resource,
                event='Failure',
                value=value,
                raw_data=raw_data
            )
        else:
            self.log.error('Got error from the endpoint check, but '
                           'cannot report it to alerta')


class EndpointMonitorServer:

    log = logging.getLogger('apimon.EndpointMonitorServer')

    def __init__(self, config):
        self.config = config
        self._running = False

        self.run_lock = threading.Lock()

        self.command_map = dict(
            stop=self.stop,
            pause=self.pause,
            resume=self.resume,
        )
        command_socket = self.config.get_default(
            'epmon', 'socket',
            '/var/lib/apimon/epmon.socket')
        self.command_socket = commandsocket.CommandSocket(command_socket)

        self._command_running = False

        self._monitors = {}
#        self.accepting_work = False

    def _connect_alerta(self) -> None:
        if alerta_client:
            alerta_ep = self.config.get_default('alerta', 'endpoint')
            alerta_token = self.config.get_default('alerta', 'token')
            if alerta_ep and alerta_token:
                self.alerta = alerta_client(
                    endpoint=alerta_ep,
                    key=alerta_token)

    def start(self):
        self._running = True
        self._command_running = True
        self.log.debug("Starting command processor")

        self.command_socket.start()
        self.command_thread = threading.Thread(
            target=self.run_command, name='command')
        self.command_thread.daemon = True
        self.command_thread.start()

        self._connect_alerta()

        for cl in self.config.get_default('epmon', 'clouds', []):
            self.log.debug('Need to monitor cloud %s' % cl)
            self._monitors[cl] = EndpointMonitor(self.config, cl, self.alerta)
            self._monitors[cl].start()

    def stop(self):
        self.log.debug("Stopping")

        with self.run_lock:
            self._running = False
            self._command_running = False
            monitors = list(self._monitors.values())

        self.command_socket.stop()
        for mon in monitors:
            try:
                mon.stop()
                mon.join()
            except Exception:
                self.log.exception("Exception stoping monitoring thread")

        self.log.debug("Stopped")

    def join(self):
        pass

    def pause(self):
        self.log.debug('Pausing')

        with self.run_lock:
            monitors = list(self._monitors.values())

        for mon in monitors:
            mon.pause()

    def resume(self):
        self.log.debug('Resuming')

        with self.run_lock:
            monitors = list(self._monitors.values())

        for mon in monitors:
            mon.resume()

    def run_command(self):
        while self._command_running:
            try:
                command = self.command_socket.get().decode('utf8')
                if command != '_stop':
                    self.command_map[command]()
            except Exception:
                self.log.exception("Exception while processing command")
