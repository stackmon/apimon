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
import sys
import json


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


class ManagementEvent(object):
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


class ResultEvent(object):
    """Result event"""
    pass


class JobCompletedEvent(ResultEvent):
    """Job completed"""
    def __init__(self, job, result):
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

    def stop(self):
        self.log.debug('Stopping Git thread')
        self._stopped = True
        self.wake_event.set()

    def run(self):
        self.log.debug('Starting watching git repository')
        while True:
            self.wake_event.wait(
                self.config.get_default('scheduler', 'refresh_interval', 10))
            if self._stopped:
                return
            for name, project in self.projects.items():
                if project.is_repo_update_necessary():
                    project.refresh_git_repo()
                    self.scheduler._git_updated(project)


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
        self.result_event_queue = _queue.UniqueQueue()

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
        for item in self.config.get_section('test_projects'):
            prj = Project(
                name=item.get('name'),
                repo_url=item.get('repo_url'),
                repo_ref=item.get('repo_ref'),
                project_type=item.get('type'),
                location=item.get('scenarios_location'),
                exec_cmd=item.get('exec_cmd'),
                work_dir=self.config.get_default('scheduler',
                                                 'work_dir', 'wrk'),
                env=item.get('env'),
                scenarios=item.get('scenarios')
            )
            prj.get_git_repo()
            self._projects[prj.name] = prj

    def _load_environments(self) -> None:
        """Load test environments"""
        for item in self.config.get_section('test_environments'):
            env = TestEnvironment(
                config=self.config,
                clouds_config=self._clouds,
                **item
            )
            self._environments[env.name] = env

    def _load_clouds(self) -> None:
        """Load cloud connections"""
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

        self.__executor_client.start()

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
        self.wake_event.set()
        self._git_refresh_thread.join()
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

                while (not self.result_event_queue.empty() and
                       not self._stopped):
                    self.process_result_queue()

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

    def process_result_queue(self) -> None:
        self.log.debug('Fetching result event')
        event = self.result_event_queue.get()
        self.log.debug('Processing result event %s' % event)
        try:
            if isinstance(event, JobCompletedEvent):
                self._process_completed_job(event)
            else:
                self.log.error('Can not process result event %s' % event)
        except Exception:
            self.log.exception('Exception in the result event:')
            event.exception(sys.exc_info())
        self.result_event_queue.task_done()

    def _reconfig(self, event) -> None:
        self.__executor_client.pause_rescheduling()
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

        self.__executor_client.resume_rescheduling()

    def _process_completed_event(self) -> None:
        pass

    def _pause_scheduling(self, event) -> None:
        self.__executor_client.pause_rescheduling()

    def _resume_scheduling(self, event) -> None:
        self.__executor_client.resume_rescheduling()

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
