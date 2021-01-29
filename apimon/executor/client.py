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
import json
import os
import threading
import time

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


class ScheduleWatcher(threading.Thread):
    """A thread that get's prepared tasks and schedules them to gearman

    On a callback (i.e. broken connection) we have not much capabilities
    to reschedule task again, instead it is being placed into queue
    and this thread is taking care of passing them to gearman again
    """

    log = logging.getLogger('apimon.RescheduleWatcher')

    def __init__(self, client, matrix):
        threading.Thread.__init__(self)
        self.daemon = True
        self.client = client
        self.matrix = matrix
        self._stopped = False
        self._paused = False
        self.wake_event = threading.Event()

        self._tick_interval = 1  # Seconds
        self._report_queue_size_ticks = 600
        self._report_queue_size_cnt = 0

    def stop(self) -> None:
        self._stopped = True
        self.wake_event.set()

    def pause(self) -> None:
        self._paused = True

    def unpause(self) -> None:
        self._paused = False

    def run(self) -> None:
        while True:
            self.wake_event.wait(self._tick_interval)
            if self._stopped:
                return

            if self._paused:
                continue
            # 1. find item next on schedule
            # 2. prepare job
            # 3. send job
            for task in list(self.matrix.tasks()):
                current_time = datetime.datetime.now()
                if not task.scheduled_at and task.next_run_at <= current_time:
                    # unless it is already "running" and time is up - start it
                    self.client._submit_gear_task(task)

            if self._report_queue_size_cnt == self._report_queue_size_ticks:
                statsd = self.client.scheduler.statsd
                if statsd:
                    self.matrix.report_stats(
                        statsd)
                self._report_queue_size_cnt = 0
            self._report_queue_size_cnt += 1


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
                if task._gearman_job.handle == handle:
                    self.__apimon_gear.onUnknownJob(job)


class JobExecutorClient(object):
    """It is responsible for scheduling tasks for execution and send Gear jobs
    for those
    """
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

        self._matrix = Matrix()

        self._scheduler_thread = ScheduleWatcher(self, self._matrix)
        self._scheduler_thread.start()
        self._cleanup_thread = GearmanCleanup(self)
        self._cleanup_thread.start()
        self.stopped = False

        self.statsd = scheduler.statsd

    def start(self) -> None:
        self.log.debug('Starting scheduling of jobs')

    def stop(self) -> None:
        self.log.debug('Stopping')
        self.stopped = True
        self._reschedule = False
        self._scheduler_thread.stop()
        self._cleanup_thread.stop()
        self._scheduler_thread.join()
        self._cleanup_thread.join()
        if self.statsd:
            self._matrix.report_stats(self.statsd, zero=True)

        self.log.debug('Stopped')

    def _submit_gear_task(self, task, **kwargs) -> None:
        task.prepare_gear_job(self.config,
                              self.scheduler._config_version)
        if task._gear_job_id:
            self.__tasks[task._gear_job_id] = task

        if task._gearman_job:
            while not self.stopped:
                try:
                    self.gearman.submitJob(task._gearman_job, timeout=30,
                                           **kwargs)
                    task.scheduled_at = datetime.datetime.now()
                    task.next_run_at = task.scheduled_at + task.interval_dt
                    self.scheduler.task_scheduled(task)
                    return
                except Exception:
                    self.log.debug('Exception submitting job')
                    time.sleep(2)

    def onTaskCompleted(self, job, result=None) -> JobTask:
        self.log.debug('Task %s completed with %s' % (job, result))
        task = self.__tasks.get(job.unique)
        data = getJobData(job)
        if task:
            log = logutils.get_annotated_logger(self.log, task._gear_job_id,
                                                task._apimon_job_id)
            log.debug('Task returned %s back' % data)
            self.scheduler.task_completed(task)

            self.__tasks.pop(task._gear_job_id)
            task.reset_job()

        else:
            self.log.warn('No data for job found')

            try:
                if data:
                    # Try to recover using job data
                    project = data.get('project', {})
                    env = data.get('env', {})
                    if project is not None and env is not None:
                        task = self._matrix.find_neo(
                            project['name'], project['task'], env['name'])

                        if task:
                            task.reset_job()
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

    def pause_scheduling(self) -> None:
        self.log.info('Pausing Job scheduling')
        # Disable scheduling
        self._scheduler_thread.pause()
        # Cancel jobs that we can
        self._cancel_tasks()
        if self.statsd:
            self._matrix.report_stats(self.statsd, zero=True)

    def resume_scheduling(self) -> None:
        self.log.info('Resume Job scheduling')
        # Unpause the scheduler thread
        self._scheduler_thread.unpause()
        self._render_matrix()

    def _cancel_tasks(self) -> None:
        for task in list(self.__tasks.values()):
            self.log.debug('Cancelling task %s' % task)
            if not task.started:
                # The task hasn't started yet
                self.cancel_job(task)
            else:
                self.log.info('Not cancelling job %s since it is running' %
                              task._gear_job_id)
                self.__tasks.pop(task._gear_job_id, None)

    def _render_matrix(self) -> None:
        """Prepare matrix of projects/tasks/environments
        """
        # TODO(gtema): reset the matrix
        self._matrix.clear()
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
                args = {}
                if 'interval' in item:
                    args['interval'] = int(item['interval'])
                for task in project.tasks():
                    task_instance = JobTask(project, task, env, **args)
                    self._matrix.send_neo(
                        project.name, task, env.name,
                        task_instance)
            else:
                for task in item.get('tasks', []):
                    interval = 0
                    if isinstance(task, dict):
                        task_name = list(task.keys())[0]
                        interval = int(task[task_name].get('interval', 0))
                        task = task_name

                    task_instance = JobTask(
                        project,
                        os.path.join(project.location, task),
                        env, interval=interval)

                    self._matrix.send_neo(
                        project.name, task, env.name, task_instance)

        if self.statsd:
            self._matrix.report_stats(self.statsd)

    def onDisconnect(self, job) -> None:
        """Disconnect called for each job with the lost server"""
        # Get the job that failed and "re-schedule" it
        # disconnect means we will never hear anything from the job
        task = self.__tasks[job.unique]
        if task:
            task.reset_job()

    def onUnknownJob(self, job) -> None:
        self.onTaskCompleted(job, 'LOST')

    def cancel_job(self, task) -> None:
        """Try to cancel gear job"""
        log = logutils.get_annotated_logger(self.log, task._gear_job_id,
                                            task._apimon_job_id)
        job = task._gearman_job

        req = gear.CancelJobAdminRequest(job.handle)
        job.connection.sendAdminRequest(req, timeout=300)
        log.debug("Response to cancel task request: %s", req.response.strip())
        if req.response.startswith(b"OK"):
            try:
                self.__tasks.pop(task._gear_job_id)
                task.reset_job()
            except Exception:
                pass
            return True
        return False

    def lookForLostTasks(self) -> None:
        self.log.debug("Looking for lost tasks")
        # Construct a list from the values iterator to protect from it changing
        # out from underneath us.
        for task in list(self.__tasks.values()):
            job = task._gearman_job
            if not job.handle:
                # The task hasn't been enqueued yet
                continue
            p = gear.Packet(gear.constants.REQ, gear.constants.GET_STATUS,
                            job.handle)
            job.connection.sendPacket(p)

    def _project_updated(self, project) -> None:
        """Check whether all tasks in the project are scheduled"""
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
                    env = self.scheduler._environments.get(item['env'])
                    task_instance = JobTask(project, task, env)

                    self._matrix.send_neo(
                        project.name,
                        os.path.join(project.location, task),
                        env.name, task_instance)

        if self.statsd:
            self._matrix.report_stats(self.statsd)
