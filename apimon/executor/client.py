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
import json
import random
import string
import threading
import queue
import time
import uuid

import gear


from apimon.model import JobTask, Matrix
from apimon.lib import logutils


def getJobData(job) -> dict:
    if not len(job.data):
        return {}
    d = job.data[-1]
    if not d:
        return {}
    return json.loads(d)


def generate_job_id() -> str:
    return ''.join([random.choice(string.ascii_letters + string.digits) for n
                    in range(12)])


class GearmanCleanup(threading.Thread):
    """A thread that checks to see if outstanding jobs have
    completed without reporting back. """
    log = logging.getLogger("apimon.GearmanCleanup")

    def __init__(self, gearman):
        threading.Thread.__init__(self)
        self.daemon = True
        self.gearman = gearman
        self.wake_event = threading.Event()
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True
        self.wake_event.set()

    def run(self) -> None:
        while True:
            self.wake_event.wait(300)
            if self._stopped:
                return
            try:
                self.gearman.lookForLostTasks()
            except Exception:
                self.log.exception("Exception checking builds:")


class TaskRescheduler(threading.Thread):
    """A thread that get's prepared tasks and schedules them to gearman

    On a callback (i.e. broken connection) we have not much capabilities
    to reschedule task again, instead it is being placed into queue
    and this thread is taking care of passing them to gearman again
    """

    log = logging.getLogger('apimon.TaskRescheduler')

    def __init__(self, client, tasks):
        threading.Thread.__init__(self)
        self.daemon = True
        self.client = client
        self.tasks = tasks
        self._stopped = False
        self.wake_event = threading.Event()

    def stop(self) -> None:
        self._stopped = True
        self.wake_event.set()

    def run(self) -> None:
        while True:
            self.wake_event.wait(300)
            try:
                item = self.tasks.get(False)
                if item:
                    # We wait inside to ensure we schedule successfully,
                    # so no need for any checks here
                    self.client._submit_task(item)
                    self.tasks.task_done()
            except queue.Empty:
                self.wake_event.clear()
                pass

            if self._stopped:
                return


class ApimonGearClient(gear.Client):
    def __init__(self, apimon_gear):
        super(ApimonGearClient, self).__init__('apimon.GermanClient')
        self.__apimon_gear = apimon_gear

    def handleWorkComplete(self, packet) -> gear.Job:
        job = super(ApimonGearClient, self).handleWorkComplete(packet)
        self.__apimon_gear.onTaskCompleted(job)
        return job

    def handleWorkFail(self, packet) -> gear.Job:
        job = super(ApimonGearClient, self).handleWorkFail(packet)
        self.__apimon_gear.onTaskCompleted(job)
        return job

    def handleWorkException(self, packet) -> gear.Job:
        job = super(ApimonGearClient, self).handleWorkException(packet)
        self.__apimon_gear.onTaskCompleted(job)
        return job

    def handleWorkStatus(self, packet) -> gear.Job:
        job = super(ApimonGearClient, self).handleWorkStatus(packet)
        self.__apimon_gear.onWorkStatus(job)
        return job

    def handleWorkData(self, packet) -> gear.Job:
        job = super(ApimonGearClient, self).handleWorkData(packet)
        self.__apimon_gear.onWorkStatus(job)
        return job

    def handleDisconnect(self, job) -> None:
        job = super(ApimonGearClient, self).handleDisconnect(job)
        self.__apimon_gear.onDisconnect(job)

    def handleStatusRes(self, packet) -> None:
        try:
            job = super(ApimonGearClient, self).handleStatusRes(packet)
        except gear.UnknownJobError:
            handle = packet.getArgument(0)
            self.log.error('Got status update for unknown job')
            for task in self.__apimon_gear.__tasks.values():
                if task.__gearman_job.handle == handle:
                    self.__apimon_gear.onUnknownJob(job)


class JobExecutorClient(object):
    log = logging.getLogger('apimon.JobExecutorClient')

    def __init__(self, config, scheduler):
        self.config = config
        self.scheduler = scheduler
        self.__tasks = {}

        self.gearman = ApimonGearClient(self)
        for server in config.get_section('gear'):
            self.gearman.addServer(server.get('host'), server.get('port'),
                                   server.get('ssl_key'),
                                   server.get('ssl_cert'),
                                   server.get('ssl_ca'),
                                   keepalive=True, tcp_keepidle=60,
                                   tcp_keepintvl=30, tcp_keepcnt=5)

        self._task_queue = queue.Queue()
        self._rescheduler_thread = TaskRescheduler(self, self._task_queue)
        self._rescheduler_thread.start()
        self._cleanup_thread = GearmanCleanup(self)
        self._cleanup_thread.start()
        self.stopped = False
        self._reschedule = True

        self._matrix = Matrix()

    def start(self) -> None:
        self.log.debug('Starting scheduling of jobs')

    def stop(self) -> None:
        self.log.debug('Stopping')
        self.stopped = True
        self._reschedule = False
        self._rescheduler_thread.stop()
        self._cleanup_thread.stop()
        self._rescheduler_thread.join()
        self._cleanup_thread.join()
        self.log.debug('Stopped')

    def _submit_task(self, task, **kwargs) -> None:
        if task.id:
            self.__tasks[task.id] = task
            self._matrix.send_neo(task.project.name, task.task,
                                  task.env.name, task.id)

        if task.__gearman_job:
            while not self.stopped:
                try:
                    self.gearman.submitJob(task.__gearman_job, timeout=30,
                                           **kwargs)
                    self.scheduler.task_scheduled(task)
                    return
                except Exception:
                    self.log.debug('Exception submitting job')
                    time.sleep(2)

    def _schedule_task(self, project, task,
                       env, wake_rescheduler=True) -> None:
        new_task = self._prepare_task(project, task, env)
        if new_task:
            self.__put_task_to_queue(new_task, wake_rescheduler)

    def __put_task_to_queue(self, task, wake_rescheduler=True) -> None:
        self._task_queue.put(task)
        if wake_rescheduler:
            self._rescheduler_thread.wake_event.set()

    def onTaskCompleted(self, job, result=None) -> JobTask:
        self.log.debug('Task %s completed with %s' % (job, result))
        task = self.__tasks.get(job.unique)
        data = getJobData(job)
        if task:
            log = logutils.get_annotated_logger(self.log, task.id, task.job_id)
            log.debug('Task returned %s back' % data)
            self.scheduler.task_completed(task)
            del self.__tasks[task.id]

            if not self.stopped and self._reschedule:
                # otherwise we might want to clean waiting queue, but it is
                # processed immediately, so no chance to get something there
                self._schedule_task(task.project, task.task, task.env)
        else:
            self.log.debug('No data for job found')
            try:
                if data:
                    project = data.get('project', {})
                    env = data.get('env', {})
                    if project is not None and env is not None:
                        self._scheduler_task(
                            self.scheduler._projects.get(project['name']),
                            project['name'], project['task'], env['name'])
            except Exception:
                self.log.exception('Exception during recovering lost job')
        return task

    def onWorkStatus(self, job) -> None:
        data = getJobData(job)
        self.log.debug('Job %s got update: %s', job, data)
        task = self.__tasks.get(job.unique)
        if task:
            if not task.started:
                self.log.info('Task %s started', task)
                task.started = True
                self.scheduler.task_started(task)

    def _prepare_task(self, project, task, env) -> JobTask:
        self.log.debug('Preparing gearman Job %s:%s:%s', project, task, env)
        if not project.is_task_valid(task):
            return None

        task = JobTask(uuid.uuid4().hex,
                       project, task, env)
        task.job_id = generate_job_id()

        gearman_job = gear.TextJob(
            'apimon:ansible',
            json.dumps(task.get_job_data(self.config,
                                         self.scheduler._config_version)),
            unique=task.id
        )

        task.__gearman_job = gearman_job
        task.__gearman_worker = None

        return task

    def pause_rescheduling(self) -> None:
        self.log.debug('Pausing Job scheduling')
        self._reschedule = False
        self._cancel_tasks()

    def resume_rescheduling(self) -> None:
        self.log.debug('Resume Job scheduling')
        if self._reschedule:
            self.log.debug('Rescheduling was not enabled. Not doing anything')
            return
        self._reschedule = True
        self._schedule_all()

    def _cancel_tasks(self) -> None:
        for task in list(self.__tasks.values()):
            self.log.debug('Cancelling task %s' % task)
            if not task.started:
                # The task hasn't started yet
                self.cancelJobInQueue(task)
            else:
                self.log.info('Not cancelling job %s since it is running' %
                              task.id)
                self.__tasks.pop(task.id, None)
                self._matrix.send_neo(task.project.name, task.task,
                                      task.env.name, '')

    def _schedule_all(self) -> None:
        self._render_matrix()
        self.log.debug('World Matrix looks like %s' % self._matrix)

        for (project, task, env) in self._matrix.find_glitches():
            self.log.debug('glitch at %s %s %s', project, task, env)
            self._schedule_task(
                self.scheduler._projects.get(project),
                task,
                self.scheduler._environments.get(env),
                False)
        self._rescheduler_thread.wake_event.set()

    def _render_matrix(self) -> None:
        """Prepare matrix of projects/tasks/environments
        """
        matrix = self.config.get_section('test_matrix') or []
        for item in matrix:
            project = self.scheduler._projects.get(item['project'])
            if not project:
                raise ValueError('Project %s referred in matrix is not known',
                                 item['project'])
            env = self.scheduler._environments.get(item['env'])
            if not env:
                raise ValueError('Environment %s referred in matrix is not '
                                 'known', item['env'])
            if 'tasks' not in item:
                for task in project.tasks():
                    self._matrix.send_neo(project.name, task, env.name)
            else:
                for task in item.get('tasks', []):
                    self._matrix.send_neo(project.name, task, env.name)

    def onDisconnect(self, job) -> None:
        """Disconnect called for each job with the lost server"""
        # Get the job that failed and "re-schedule" it
        task = self.__tasks[job.unique]
        self.__put_task_to_queue(task, True)

    def onUnknownJob(self, job) -> None:
        self.onTaskCompleted(job, 'LOST')

    def cancelJobInQueue(self, task) -> None:
        log = logutils.get_annotated_logger(self.log, task.id, task.job_id)
        job = task.__gearman_job

        req = gear.CancelJobAdminRequest(job.handle)
        job.connection.sendAdminRequest(req, timeout=300)
        log.debug("Response to cancel task request: %s", req.response.strip())
        if req.response.startswith(b"OK"):
            try:
                del self.__tasks[task.id]
                self._matrix.send_neo(task.project.name, task.task,
                                      task.env.name, '')

            except Exception:
                pass
            return True
        return False

    def lookForLostTasks(self) -> None:
        self.log.debug("Looking for lost tasks")
        # Construct a list from the values iterator to protect from it changing
        # out from underneath us.
        for task in list(self.__tasks.values()):
            job = task.__gearman_job
            if not job.handle:
                # The task hasn't been enqueued yet
                continue
            p = gear.Packet(gear.constants.REQ, gear.constants.GET_STATUS,
                            job.handle)
            job.connection.sendPacket(p)

    def _project_updated(self, project) -> None:
        """Check whether all tasks in the project are
        scheduled
        """
        current_tasks = []
        for task in list(self.__tasks.values()):
            if task.project.name != project.name:
                # not interesting
                continue
            current_tasks.append(task.task)
        # Get all tasks which are not scheduled
        self.log.debug('Current running tasks: %s' % current_tasks)
        new_tasks = [value for value in project.tasks() if value not in
                     current_tasks]
        self.log.debug('Need to schedule newly tasks: %s' % new_tasks)
        if not new_tasks:
            return

        matrix = self.config.get_section('test_matrix') or []
        for item in matrix:
            if item['project'] != project.name:
                continue
            # reschedule all new tasks of this project
            if 'tasks' not in item:
                # In in the matrix we set tasks - do not scheduler newly
                # appeared ones
                for task in new_tasks:
                    self._schedule_task(
                        project, task,
                        self.scheduler._environments.get(item['env']),
                    )
