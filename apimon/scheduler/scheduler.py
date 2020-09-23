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

import datetime
import logging
import threading
import sys
import json
import queue

import influxdb
import openstack

try:
    from alertaclient.api import Client as alerta_client
except ImportError:
    alerta_client = None

from apimon.lib import queue as _queue
from apimon.lib.gearworker import GearWorker
from apimon.project import Project
from apimon.model import TestEnvironment, Cloud

from apimon.lib import commandsocket


COMMANDS = ['pause', 'resume', 'reconfig', 'stop']


class ManagementEvent:
    """Event for processing by main queue run loop"""
    def __init__(self):
        self._wait_event = threading.Event()
        self._exc_info = None

    def exception(self, exc_info):
        self._exc_info = exc_info
        self._wait_event.set()

    def done(self):
        self._wait_event.set()

    def wait(self, timeout=None):
        self._wait_event.wait(timeout)
        if self._exc_info:
            # sys.exc_info returns (type, value, traceback)
            type_, exception_instance, traceback = self._exc_info
            raise exception_instance.with_traceback(traceback)
        return self._wait_event.is_set()


class ReconfigureEvent(ManagementEvent):
    """Reconfiguration"""
    def __init__(self, config):
        super(ReconfigureEvent, self).__init__()
        self.config = config


class PauseSchedulingEvent(ManagementEvent):
    """Pause"""
    def __init__(self):
        super(PauseSchedulingEvent, self).__init__()


class ResumeSchedulingEvent(ManagementEvent):
    """Resume Scheduling"""
    def __init__(self):
        super(ResumeSchedulingEvent, self).__init__()


class GitRefreshEvent(ManagementEvent):
    """Git got new change, update our state"""
    def __init__(self, project):
        super(GitRefreshEvent, self).__init__()
        self.project = project


class OperationalEvent:
    """Result event"""
    pass


class JobScheduledEvent(OperationalEvent):
    """Job scheduled"""
    def __init__(self, job):
        super(JobScheduledEvent, self).__init__()


class JobStartedEvent(OperationalEvent):
    """Job started"""
    def __init__(self, job):
        super(JobStartedEvent, self).__init__()


class JobCompletedEvent(OperationalEvent):
    """Job completed"""
    def __init__(self, job):
        super(JobCompletedEvent, self).__init__()


class GitRefresh(threading.Thread):
    """A thread taking care of refreshing git checkout"""
    log = logging.getLogger('apimon.GitRefresh')

    def __init__(self, scheduler, projects, config):
        threading.Thread.__init__(self)
        self.scheduler = scheduler
        self.projects = projects
        self.config = config

        self.wake_event = threading.Event()
        self._stopped = False
        self._paused = False

    def stop(self):
        self.log.debug('Stopping Git thread')
        self._stopped = True
        self.wake_event.set()

    def pause(self):
        self.log.debug('Pausing Git thread')
        self._paused = True
        self.wake_event.set()

    def unpause(self):
        self.log.debug('Unpausing Git thread')
        self._paused = False
        self.wake_event.set()

    def run(self):
        self.log.debug('Starting watching git repository')
        while True:
            self.wake_event.wait(
                self.config.get_default('scheduler', 'refresh_interval', 10))
            self.wake_event.clear()

            if self._stopped:
                return

            if self._paused:
                continue

            for name, project in self.projects.items():
                if self._paused:
                    continue

                if project.is_repo_update_necessary():
                    try:
                        project.refresh_git_repo()
                        self.scheduler._git_updated(project)
                    except Exception:
                        self.log.exception('Exception during updating git '
                                           'repo')


class ProjectCleanup(threading.Thread):
    """A thread taking care of periodical project cleanup"""
    log = logging.getLogger('apimon.ProjectCleanup')

    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config

        self.wake_event = threading.Event()
        self._stopped = False
        self._paused = False

        self._clouds = []

    def stop(self):
        self.log.debug('Stopping ProjectCleanup thread')
        self._stopped = True
        self.wake_event.set()

    def pause(self):
        self.log.debug('Pause ProjectCleanup thread')
        self._paused = True
        self.wake_event.set()

    def unpause(self):
        self.log.debug('Unpause ProjectCleanup thread')
        self._paused = False
        self.wake_event.set()

    def run(self):
        # Determine clouds to cleanup by looking into epmon clouds
        for cl in self.config.get_default('epmon', 'clouds', []):
            if isinstance(cl, dict):
                if len(cl.items()) != 1:
                    raise RuntimeError(
                        'Can not parse epmon clouds configuration')
                target_cloud = list(cl.keys())[0]
            else:
                target_cloud = cl
            self._clouds.append(target_cloud)

        while True:
            self.wake_event.wait(
                self.config.get_default('scheduler', 'cleanup_interval', 1200))
            self.wake_event.clear()

            if self._stopped:
                return

            if self._paused:
                continue

            for cloud in self._clouds:
                self._project_cleanup(cloud)

    def _get_cloud_connect(self, cloud):
        auth_part = {}
        for cnf in self.config.config.get('clouds', []):
            if cnf.get('name') == cloud:
                auth_part = cnf.get('data')
        if not auth_part:
            self.log.error('Cannot determine cloud configuration for the '
                           'project cleanup of %s' % cloud)
            return None
        region = openstack.config.get_cloud_region(
            load_yaml_config=False,
            **auth_part)
        try:
            conn = openstack.connection.Connection(
                config=region,
            )
            return conn
        except Exception:
            self.log.exception('Cannot establish connection to cloud %s' %
                               cloud)

    def _project_cleanup(self, target_cloud):
        conn = self._get_cloud_connect(target_cloud)
        if not conn:
            self.log.error('Cannot do project cleanup since connection n/a')
            return
        age = datetime.timedelta(hours=6)
        current_time = datetime.datetime.now()
        created_at_filter = current_time - age
        _filters = {'created_at': created_at_filter.isoformat()}
        try:
            self.log.debug('Performing project cleanup in %s' % target_cloud)
            conn.project_cleanup(
                wait_timeout=600,
                filters=_filters
            )
        except Exception:
            self.log.exception('Exception during project cleanup')


class Stat(threading.Thread):
    """A thread taking care of reporting statistics"""
    log = logging.getLogger('apimon.stat')

    def __init__(self, scheduler, config):
        threading.Thread.__init__(self)
        self.scheduler = scheduler
        self.config = config

        self.metric_queue = queue.Queue()

        self.wake_event = threading.Event()
        self._stopped = False

    def stop(self):
        self.log.debug('Stopping stat thread')
        self._stopped = True
        self.wake_event.set()

    def add_metrics(self, data: dict) -> None:
        """Register metrics in the queue"""
        self.metric_queue.put(data)

    def _connect_influx(self) -> None:
        try:
            influx_cfg = self.config.get_section('metrics', 'influxdb')
            if influx_cfg:
                self.influxdb_client = influxdb.InfluxDBClient(
                    influx_cfg.get('host', 'localhost'),
                    influx_cfg.get('port', '8186'),
                    influx_cfg.get('username'),
                    influx_cfg.get('password'),
                )
        except Exception:
            self.log.exception('Failed to establish connection to influxDB')

    def run(self):
        self._connect_influx()
        while True:
            self.wake_event.wait(
                self.config.get_default('stat', 'interval', 5))
            if self._stopped:
                return

            metrics = []

            while True:
                try:
                    metric = self.metric_queue.get(False)
                    if metric:
                        metrics.append(metric)
                    metric.task_done()
                except queue.Empty:
                    break

            if metrics and self._influxdb_client:
                self.log.debug('Metrics :%s' % metrics)
                try:
                    self._influxdb_client.write_points(metrics)
                except Exception:
                    self.log.exception('Error writing metrics to influx')


class Scheduler(threading.Thread):

    log = logging.getLogger('apimon.Scheduler')

    def __init__(self, config):
        threading.Thread.__init__(self)
        self.daemon = True
        self.wake_event = threading.Event()
        self.run_handler_lock = threading.Lock()

        self.command_map = {
            'pause': self.pause,
            'resume': self.resume,
            'reconfig': self.reconfigure,
            'stop': self.stop
        }

        self.config = config

        self.management_event_queue = _queue.UniqueQueue()
        self.operational_event_queue = _queue.UniqueQueue()

        command_socket = self.config.get_default(
            'scheduler', 'socket', '/var/lib/apimon/scheduler.socket')
        self._command_socket = commandsocket.CommandSocket(command_socket)

        self._alerta = None
        self._projects = {}
        self._environments = {}
        self._clouds = {}
        self._clouds_config = {}
        self._config_version = 0

        self._git_refresh_thread = GitRefresh(self, self._projects,
                                              self.config)
        self._project_cleanup_thread = ProjectCleanup(self.config)
        # self._stat_thread = Stat(self, self.config)

        self.cloud_config_gearworker = GearWorker(
            'APImon Scheduler',
            'apimon.ConfigWorker',
            'config_worker',
            self.config,
            {'apimon:get_cloud_config': self.get_cloud_config},
        )

    def socket_run(self) -> None:
        """Command socket run function"""
        while self._socket_running:
            try:
                command = self._command_socket.get().decode('utf8')
                if command != '_stop':
                    self.command_map[command]()
            except Exception:
                self.log.exception("Exception while processing command")

    def _load_projects(self) -> None:
        """Load projects from config and initialize them
        """
        self._projects.clear()
        for item in self.config.get_section('test_projects'):
            project_args = {
                'name': item.get('name'),
                'repo_url': item.get('repo_url'),
                'work_dir': self.config.get_default(
                    'scheduler', 'work_dir', 'wrk')
            }
            if 'repo_ref' in item:
                project_args['repo_ref'] = item.get('repo_ref')
            if 'type' in item:
                project_args['type'] = item.get('type')
            if 'location' in item:
                project_args['location'] = \
                    item.get('location')
            if 'exec_cmd' in item:
                project_args['exec_cmd'] = item.get('exec_cmd')
            if 'env' in item:
                project_args['env'] = item.get('env')
            if 'scenarios' in item:
                project_args['scenarios'] = item.get('scenarios')

            prj = Project(**project_args)
            prj.get_git_repo()
            self._projects[prj.name] = prj

    def _load_environments(self) -> None:
        """Load test environments"""
        self._environments.clear()
        for item in self.config.get_section('test_environments'):
            env = TestEnvironment(
                config=self.config,
                clouds_config=self._clouds,
                **item
            )
            self._environments[env.name] = env

    def _load_clouds(self) -> None:
        """Load cloud connections"""
        self._clouds.clear()
        for item in self.config.get_section('clouds'):
            cl = Cloud(
                name=item.get('name'),
                data=item.get('data')
            )
            self._clouds[cl.name] = cl

    def _load_clouds_config(self) -> None:
        """Load clouds configuration from the config"""
        clouds = {}
        conf = {}
        for item in self.config.get_section('clouds'):
            clouds[item.get('name')] = item.get('data')
        conf['clouds'] = clouds

        influx_conf = self.config.get_default('metrics', 'influxdb', {})
        if influx_conf:
            conf['metrics'] = {
                'influxdb': influx_conf
            }
        conf['_version'] = self._config_version
        self._clouds_config = conf

    def start(self) -> None:
        super(Scheduler, self).start()
        self._stopped = False

        self._load_projects()
        self._load_clouds()
        self._load_environments()

        self._git_refresh_thread.start()
        self._project_cleanup_thread.start()

        self.__executor_client.start()
        # self._stat_thread.start()

        self._socket_running = True
        self._command_socket.start()
        self.__socket_thread = threading.Thread(target=self.socket_run,
                                                name='command')
        self.__socket_thread.daemon = True
        self.__socket_thread.start()

        self.cloud_config_gearworker.start()

    def stop(self) -> None:
        self._stopped = True
        self.__executor_client.stop()
        self._git_refresh_thread.stop()
        self._project_cleanup_thread.stop()
        self.wake_event.set()
        self._git_refresh_thread.join()
        self._project_cleanup_thread.join()
        # self._stat_thread.join()
        self._socket_running = False

        self.cloud_config_gearworker.stop()
        self._command_socket.stop()
        self.__socket_thread.join()

    def exit(self) -> None:
        self.log.debug("Prepare to exit")
        self.wake_event.set()
        self.log.debug("Waiting for exit")

    def reconfigure(self, config=None) -> None:
        self.log.debug('Reconfiguration')
        if not config:
            config = self.config.read()
        event = ReconfigureEvent(config)
        self.management_event_queue.put(event)
        self.wake_event.set()
        self.log.debug('Waiting for reconfiguration')
        event.wait()
        self.log.debug('Reconfiguration complete')

    def pause(self) -> None:
        self.log.debug('Pausing scheduling')
        event = PauseSchedulingEvent()
        self.management_event_queue.put(event)
        self.wake_event.set()
        event.wait()

    def resume(self) -> None:
        self.log.debug('Resume scheduling')
        event = ResumeSchedulingEvent()
        self.management_event_queue.put(event)
        self.wake_event.set()
        event.wait()

    def task_scheduled(self, task) -> None:
        event = JobScheduledEvent(task)
        self.operational_event_queue.put(event)
        self.wake_event.set()

    def task_started(self, task) -> None:
        event = JobStartedEvent(task)
        self.operational_event_queue.put(event)
        self.wake_event.set()

    def task_completed(self, task) -> None:
        event = JobCompletedEvent(task)
        self.operational_event_queue.put(event)
        self.wake_event.set()

    def set_executor(self, executor) -> None:
        self.__executor_client = executor

    def run(self) -> None:
        """Main event function"""
        while True:
            self.log.debug('Main handler sleeping')
            self.wake_event.wait()
            self.wake_event.clear()
            if self._stopped:
                self.log.info('Main handler stopping')
                return
            self.log.debug('Main handler awake')
            self.run_handler_lock.acquire()
            try:
                while (not self.management_event_queue.empty() and
                       not self._stopped):
                    self.process_management_queue()

                while (not self.operational_event_queue.empty() and
                       not self._stopped):
                    self.process_operational_queue()

            except Exception:
                self.log.exception('Exception in main handler')
                self.wake_event.set()
            finally:
                self.run_handler_lock.release()

    def process_management_queue(self) -> None:
        self.log.debug("Fetching management event")
        event = self.management_event_queue.get()
        self.log.debug("Processing management event %s" % event)
        try:
            if isinstance(event, ReconfigureEvent):
                self._reconfig(event)
            elif isinstance(event, PauseSchedulingEvent):
                self._pause_scheduling(event)
            elif isinstance(event, ResumeSchedulingEvent):
                self._resume_scheduling(event)
            else:
                self.log.error("Unable to handle event %s" % event)
            event.done()
        except Exception:
            self.log.exception("Exception in management event:")
            event.exception(sys.exc_info())
        self.management_event_queue.task_done()

    def process_operational_queue(self) -> None:
        self.log.debug('Fetching result event')
        event = self.operational_event_queue.get()
        self.log.debug('Processing operational event %s' % event)
        try:
            if isinstance(event, JobScheduledEvent):
                self._process_scheduled_task(event)
            elif isinstance(event, JobStartedEvent):
                self._process_started_task(event)
            elif isinstance(event, JobCompletedEvent):
                self._process_completed_task(event)
            else:
                self.log.error('Can not process operational event %s' % event)
        except Exception:
            self.log.exception('Exception in the operational event:')
            event.exception(sys.exc_info())
        self.operational_event_queue.task_done()

    def _reconfig(self, event) -> None:
        self.__executor_client.pause_scheduling()

        self._git_refresh_thread.stop()
        self._project_cleanup_thread.stop()
        self._git_refresh_thread.join()
        self._project_cleanup_thread.join()

        self.config = event.config

        if alerta_client:
            alerta_ep = self.config.get_default('alerta', 'endpoint')
            alerta_token = self.config.get_default('alerta', 'token')
            if alerta_ep and alerta_token:
                self.alerta = alerta_client(
                    endpoint=alerta_ep,
                    key=alerta_token)

        self._config_version += 1
        self._load_clouds_config()
        self._load_projects()
        self._load_environments()

        self._git_refresh_thread = GitRefresh(self, self._projects,
                                              self.config)
        self._project_cleanup_thread = ProjectCleanup(self.config)
        self._git_refresh_thread.start()
        self._project_cleanup_thread.start()

        self.__executor_client.resume_scheduling()

    def _process_scheduled_task(self, event) -> None:
        pass

    def _process_started_task(self, event) -> None:
        pass

    def _process_completed_task(self, event) -> None:
        pass

    def _pause_scheduling(self, event) -> None:
        self.__executor_client.pause_scheduling()

    def _resume_scheduling(self, event) -> None:
        self.__executor_client.resume_scheduling()

    def _git_updated(self, project) -> None:
        """We got new git revision of project"""
        self.__executor_client._project_updated(project)

    def wake_up(self) -> None:
        """Wake main processing loop up"""
        self.wake_event.set()

    def get_cloud_config(self, job) -> None:
        """Respond to the job request for getting clouds.yaml content"""
        args = json.loads(job.arguments)
        self.log.debug('Cloud config requested %s' % args)
        if not self._clouds_config:
            self._load_clouds_config()
        job.sendWorkComplete(json.dumps(self._clouds_config))
