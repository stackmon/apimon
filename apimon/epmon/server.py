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
from keystoneauth1 import exceptions as keystone_exceptions

from apimon.lib import commandsocket
from apimon.lib.alerta_client import AlertaClient
from apimon.lib.statsd import get_statsd


COMMANDS = ['stop', 'pause', 'resume', 'reconfig']


class EndpointMonitor(threading.Thread):
    """A thread that checks endpoints. """
    log = logging.getLogger("apimon.EndpointMonitor")

    def __init__(
        self, config, target_cloud, zone: str = None
    ) -> None:
        threading.Thread.__init__(self)
        self.log.info('Starting watching %s cloud' % target_cloud)
        self.config = config
        self.target_cloud = target_cloud
        self.zone = zone

        self.daemon = True
        self.wake_event = threading.Event()
        self._stopped = False
        self._pause = False
        self.alerta = AlertaClient(self.config)
        self.alerta.connect()

        self.conn = None
        self.service_override = None

        self.reload()

    def stop(self) -> None:
        self.log.info(f"Stopping epmon for {self.target_cloud}")
        self._stopped = True
        self.wake_event.set()

    def pause(self) -> None:
        self._pause = True

    def resume(self) -> None:
        self._pause = False

    def reload(self) -> None:
        # Force alerta connect (reseting counter)
        self.alerta.connect(force=True)
        self.connect_retries = 0

        self.interval = int(self.config.get_default(
            'epmon', 'interval', 5))

        self.influx_cnf = self.config.get_default(
            'metrics', 'influxdb', {}).copy()

        self.statsd_extra_keys = {
            'zone': self.zone
        }
        self.statsd = get_statsd(self.config, self.statsd_extra_keys)

        # Read list of clouds we need to monitor
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
            self.log.debug(f"Need to monitor cloud {target_cloud}")

        override_measurement = self.config.get_default('epmon', 'measurement')
        if override_measurement and self.influx_cnf:
            self.influx_cnf['measurement'] = override_measurement

        # Reset openstack conn
        self._connect_cloud(force=True)

    def run(self) -> None:
        """Main loop"""
        while True:
            if self._stopped:
                return
            if not self.conn:
                # Not sure whether it works if we loose connection
                self._connect_cloud()
            try:
                if self._pause:
                    # Do not send heartbeat as well to not to forget to resume
                    continue
                # If we are connected try to perform query
                if self.conn and self.conn.config.get_auth().get_auth_state():
                    self._execute()

                self.alerta.heartbeat(
                    origin='apimon.epmon.%s.%s' % (
                        self.zone, self.target_cloud),
                    tags=['apimon', 'epmon'],
                    attributes={
                        'zone': self.zone,
                        'cloud': self.target_cloud,
                        'service': ['apimon', 'epmon'],
                    }
                )

                # Have some rest
                time.sleep(self.interval)
            except Exception:
                self.log.exception("Exception checking endpoints:")

    def _connect_cloud(self, force=False):
        """Establish cloud connection"""
        if force:
            self.connect_retries = 0
            self.conn = None
        if self.connect_retries > 1:
            self.log.warning(
                "Reached cloud reconnect limit. Not reconnecting anymore.")
            self.conn = None
            return
        else:
            self.connect_retries += 1

        cloud_config = self.config.get_cloud('clouds', self.target_cloud)

        if not cloud_config:
            raise RuntimeError(
                f"Requested cloud {self.target_cloud} is not found")

        if self.influx_cnf and 'additional_metric_tags' in cloud_config:
            self.influx_cnf['additional_metric_tags'] = \
                cloud_config['additional_metric_tags']

        # Get cloud region config
        self.region = openstack.config.get_cloud_region(
            name=self.target_cloud,
            load_yaml_config=False,
            **cloud_config)

        if self.influx_cnf:
            self.region._influxdb_config = self.influx_cnf

        statsd_config = self.config.get_default('metrics', 'statsd')
        if statsd_config:
            # Inject statsd reporter
            self.region._statsd_host = statsd_config.get('host', 'localhost')
            self.region._statsd_port = int(statsd_config.get('port', 8125))
            self.region._statsd_prefix = (
                f"openstack.api.{self.target_cloud}.{self.zone}"
            )

        # Try to connect
        try:
            self.conn = openstack.connection.Connection(
                config=self.region,
            )
            self.conn.authorize()
        except openstack.exceptions.SDKException as ex:
            self.conn = None
            self.log.error(f"Cannot connect to the cloud: {str(ex)}")
            self.send_alert('identity', 'ConnectionException', str(ex))
        except Exception as ex:
            self.log.exception(
                f"Cannot establish connection to cloud {self.target_cloud}: "
                f"{str(ex)}")
            self.send_alert('identity', 'ConnectionException', str(ex))

    def _execute(self):
        """Perform the tests"""
        eps = []
        try:
            eps = self.conn.config.get_service_catalog(
            ).get_endpoints().items()
        except keystone_exceptions.http.Unauthorized as ex:
            # If our auth expires and we can not even renew it try to trigger
            # reconnect
            self.log.error(
                f"Got Unauthorized exception while listing endpoints:"
                f"{str(ex)}"
            )
            self.conn = None
            return
        except Exception as ex:
            self.log.error(
                f"Got unexpected exception while listing endpoints: {str(ex)}")
            self.conn = None
            return

        for service, data in eps:
            if self._stopped:
                # stop faster (do not wait for remaining services to be
                # checked)
                return
            endpoint = data[0]['url']
            self.log.debug(f"Checking service {service}")
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
                            self._query_endpoint(
                                client, service, endpoint, url)
                        else:
                            self.log.error(
                                'Wrong configuration, service_override must'
                                ' be list of string urls')
                else:
                    self.log.debug(f"Skipping querying service {service}")
            else:
                self._query_endpoint(client, service, endpoint, endpoint)

    def _query_endpoint(self, client, service, endpoint, url):
        """Query concrete endpoint"""
        response = None
        error = None
        try:
            response = client.get(
                url,
                headers={'content-type': 'application/json'},
                timeout=5)
        except openstack.exceptions.HttpException as ex:
            error = ex
            self.log.error(
                f"Got HTTP exception for endpoint {url}: {str(ex)}")
        except openstack.exceptions.SDKException as ex:
            error = ex
            self.log.error(
                f"Got openstack exception for endpoint {url}: {str(ex)}")
        except Exception:
            self.log.exception(
                f"Got uncatched exception doing request to {url}")

        status_code = -1
        if response is not None:
            status_code = int(response.status_code)

        if error or status_code >= 500:
            if endpoint != url:
                query_url = openstack.utils.urljoin(endpoint, url)
            else:
                query_url = url
            result = status_code if status_code != -1 else 'Timeout(5)'
            # Construct a nice helper message for Alerta
            value = (
                f'curl -g -i -X GET {query_url} -H '
                '"X-Auth-Token: ${TOKEN}" '
                f'-H "content-type: application/json" fails ({result})'
            )
            self.send_alert(
                resource=service,
                value=value,
                raw_data=str(error.message if error else response)
            )

    def send_alert(
        self, resource: str, value: str, raw_data: str=None
    ) -> None:
        """Send alert to alerta"""
        if self.alerta.client:
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
            self.log.error(
                'Got error from the endpoint check, but '
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

    def start(self):
        self._running = True
        self._command_running = True
        self.log.debug("Starting command processor")

        self.command_socket.start()
        self.command_thread = threading.Thread(
            target=self.run_command, name='command')
        self.command_thread.daemon = True
        self.command_thread.start()

        for cl in self.config.get_default('epmon', 'clouds', []):
            if isinstance(cl, dict):
                if len(cl.items()) != 1:
                    raise RuntimeError(
                        'Can not parse epmon clouds configuration')
                target_cloud = list(cl.keys())[0]
            else:
                target_cloud = cl
            self.log.debug(f"Need to monitor cloud {target_cloud}")

            self._monitors[target_cloud] = EndpointMonitor(
                self.config, target_cloud=target_cloud, zone=self.zone)
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
