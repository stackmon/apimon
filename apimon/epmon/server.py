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
from keystoneauth1.exceptions import ClientException

try:
    from alertaclient.api import Client as alerta_client
except ImportError:
    alerta_client = None

from apimon.lib import commandsocket
from apimon.lib.statsd import get_statsd


COMMANDS = ['stop', 'pause', 'resume', 'reconfig']


class EndpointMonitor(threading.Thread):
    """A thread that checks endpoints. """
    log = logging.getLogger("apimon.EndpointMonitor")

    def __init__(self, config, target_cloud,
                 zone: str = None, alerta: dict = None) -> None:
        threading.Thread.__init__(self)
        self.log.info('Starting watching %s cloud' % target_cloud)
        self.daemon = True
        self.wake_event = threading.Event()
        self._stopped = False
        self._pause = False
        self.config = config
        self.alerta = alerta
        self.conn = None
        self.service_override = None
        self.interval = int(self.config.get_default(
            'epmon', 'interval', 5))

        self.influx_cnf = self.config.get_default(
            'metrics', 'influxdb', {}).copy()
        self.zone = zone
        self.statsd_extra_keys = {
            'zone': self.zone
        }
        self.statsd = get_statsd(
            self.config,
            self.statsd_extra_keys)
        self.target_cloud = target_cloud

        self.reload()

    def stop(self) -> None:
        self._stopped = True
        self.wake_event.set()

    def pause(self) -> None:
        self._pause = True

    def resume(self) -> None:
        self._pause = False

    def reload(self) -> None:
        for cl in self.config.get_default('epmon', 'clouds', []):
            if isinstance(cl, dict):
                if len(cl.items()) != 1:
                    raise RuntimeError(
                        'Can not parse epmon clouds configuration')
                (target_cloud, vals) = list(cl.items())[0]
            else:
                (target_cloud, vals) = cl, {}
            if target_cloud != self.target_cloud:
                continue
            self.service_override = vals.get('service_override')
            try:
                self.interval = int(vals.get('interval', 5))
            except Exception:
                self.interval = 5
            self.log.debug('Need to monitor cloud %s' % target_cloud)

        auth_part = None

        for cnf in self.config.config.get('clouds', []):
            if cnf.get('name') == self.target_cloud:
                auth_part = cnf.get('data')
                if self.influx_cnf and 'additional_metric_tags' in auth_part:
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
        if self.influx_cnf:
            self.region._influxdb_config = self.influx_cnf
        statsd_config = self.config.get_default('metrics', 'statsd')
        if statsd_config:
            # Inject statsd reporter
            self.region._statsd_host = statsd_config.get('host', 'localhost')
            self.region._statsd_port = int(statsd_config.get('port', 8125))
            self.region._statsd_prefix = (
                'openstack.api.{environment}.{zone}'
                .format(
                    environment=self.target_cloud,
                    zone=self.zone)
            )

        self._connect()

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
                            origin='apimon.epmon.%s.%s' % (
                                self.zone, self.target_cloud),
                            tags=['apimon', 'epmon']
                        )
                    except Exception:
                        self.log.exception('Error sending heartbeat')

                time.sleep(self.interval)
            except Exception:
                self.log.exception("Exception checking endpoints:")

    def _connect(self):
        try:
            self.conn = openstack.connection.Connection(
                config=self.region,
            )
        except AttributeError as e:
            # NOTE(gtema): SDK chains attribute error when calling
            # conn.authorize, but response is not present
            self.log.error('Cannot establish connection: %s' % e.__context__)
            self.send_alert('identity', e.__context__)
        except Exception as ex:
            self.log.exception('Cannot establish connection to cloud %s: %s' %
                               (self.target_cloud, ex))
            self.send_alert('identity', 'ConnectionException', str(ex))

    def _execute(self):
        eps = self.conn.config.get_service_catalog().get_endpoints().items()
        for service, data in eps:
            endpoint = data[0]['url']
            self.log.debug('Checking service %s' % service)
            srv = None
            sdk_srv = None
            try:
                srv = self.service_override.get(service)
                if not srv and service in self.service_override:
                    # If we have empty key in overrides means we might want to
                    # disable service
                    srv = {}
                sdk_srv = srv.get('service', service)
                client = getattr(self.conn, sdk_srv)
            except (KeyError, AttributeError):
                client = self.conn.config.get_session_client(service)
            if srv is not None:
                urls = srv.get('urls', [])
                if urls:
                    for url in urls:
                        if isinstance(url, str):
                            self._query_endpoint(client, service,
                                                 endpoint, url)
                        else:
                            self.log.error('Wrong configuration, '
                                           'service_override must be list of '
                                           'string urls')
                else:
                    self.log.debug('Skipping querying service %s' % service)
            else:
                self._query_endpoint(client, service, endpoint, endpoint)

    def _query_endpoint(self, client, service, endpoint, url):
        response = None
        error = None
        try:
            response = client.get(
                url,
                headers={'content-type': 'application/json'},
                timeout=5)
        except (openstack.exceptions.SDKException, ClientException) as ex:
            error = ex
            self.log.error('Got exception for endpoint %s: %s' % (url,
                                                                  ex))
        except Exception:
            self.log.exception('Got uncatched exception doing request to %s' %
                               url)

        status_code = -1
        if response is not None:
            status_code = int(response.status_code)

        if error or status_code >= 500:
            if endpoint != url:
                query_url = openstack.utils.urljoin(endpoint, url)
            else:
                query_url = url
            result = status_code if status_code != -1 else 'Timeout(5)'
            value = (
                'curl -g -i -X GET %s -H '
                '"X-Auth-Token: ${TOKEN}" '
                '-H "content-type: application/json" fails (%s)' % (
                    query_url, result)
            )
            self.send_alert(
                resource=service,
                value=value,
                raw_data=str(error.message if error else response)
            )

    def send_alert(self, resource: str, value: str,
                   raw_data: str=None) -> None:
        if self.alerta:
            self.alerta.send_alert(
                severity='critical',
                environment=self.target_cloud,
                service=['apimon', 'endpoint_monitor'],
                origin='apimon.epmon.%s.%s' % (
                    self.zone, self.target_cloud),
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

    def __init__(self, config, zone: str = None):
        self.log.info('Starting EndpoinMonitor service')
        self.config = config
        self._running = False

        self.run_lock = threading.Lock()

        self.command_map = dict(
            stop=self.stop,
            pause=self.pause,
            resume=self.resume,
            reconfig=self.reconfig
        )
        command_socket = self.config.get_default(
            'epmon', 'socket',
            '/var/lib/apimon/epmon.socket')
        self.command_socket = commandsocket.CommandSocket(command_socket)

        self._command_running = False

        self._monitors = {}

        self.alerta = None
        self.zone = zone or self.config.get_default(
            'epmon', 'zone', 'default_zone')

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
            if isinstance(cl, dict):
                if len(cl.items()) != 1:
                    raise RuntimeError(
                        'Can not parse epmon clouds configuration')
                target_cloud = list(cl.keys())[0]
            else:
                target_cloud = cl
            self.log.debug('Need to monitor cloud %s' % target_cloud)

            self._monitors[target_cloud] = EndpointMonitor(
                self.config, target_cloud=target_cloud,
                zone=self.zone, alerta=self.alerta)
            self._monitors[target_cloud].start()

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

        self.log.info("Stopped")

    def join(self):
        pass

    def pause(self):
        self.log.debug('Pausing')

        with self.run_lock:
            monitors = list(self._monitors.values())

        for mon in monitors:
            mon.pause()

    def reconfig(self, config=None) -> None:
        self.log.debug('Reconfiguration')
        if not config:
            self.config.read()
        with self.run_lock:
            monitors = list(self._monitors.values())

        for mon in monitors:
            mon.reload()

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
