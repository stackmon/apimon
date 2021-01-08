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

from datetime import timedelta, datetime
import json
import uuid

import gear


class JobTask(object):
    def __init__(
        self,
        project, task: str,
        environment, interval: int = 0,
        zone: str = None
    ):
        self.project = project
        self.task = task
        self.env = environment
        self.zone = zone if zone else 'default'
        self.interval = interval
        self.interval_dt = timedelta(minutes=interval)
        self.started = False
        self.scheduled_at = None

        self.next_run_at = datetime.now()

        self._gear_job_id = None
        self._apimon_job_id = None
        self._gearman_job = None
        self._gearman_worker = None

    def __repr__(self):
        return "<ApimonTask 0x%x ID: %s Project: %s Task: %s>" % (
            id(self),
            self._apimon_job_id,
            self.project.name,
            self.task,
        )

    def get_job_data(self, job_id, config, config_ver=None):
        data = {
            "project": {
                "name": self.project.name,
                "type": self.project.type,
                "url": self.project.repo_url,
                "ref": self.project.repo_ref,
                "commit": str(self.project.get_commit()),
                "task": self.task,
                "exec_cmd": self.project.exec_cmd,
            },
            "env": {"name": self.env.name},
            "zone": self.zone,
        }

        env = self.env.env
        if env:
            data["env"]["vars"] = env

        if job_id:
            data["job_id"] = job_id

        if config_ver:
            data["config_version"] = config_ver

        return data

    def prepare_gear_job(self, config, config_version, job_id=None):
        """Prepare GearJob for the task run"""
        if not job_id:
            job_id = uuid.uuid4().hex

        self._apimon_job_id = job_id
        self._gear_job_id = uuid.uuid4().hex

        gearman_job = gear.TextJob(
            "apimon:%s" % self.project.type,
            json.dumps(self.get_job_data(job_id, config, config_version)),
            unique=self._gear_job_id,
        )

        self._gearman_job = gearman_job
        self._gearman_worker = None

        return gearman_job

    def reset_job(self):
        self.started = False
        self.scheduled_at = None
        self._gear_job_id = None
        self._apimon_job_id = None
        self._german_job = None
        self._gearman_worker = None


class Cloud(object):
    def __init__(self, name, data, **kwargs):
        self.name = name
        self.data = data
        for k, v in kwargs.items():
            setattr(self, k, v)


class TestEnvironment(object):
    def __init__(self, config, clouds_config, name, **kwargs):
        self.name = name
        self.env = None
        if "env" in kwargs:
            self.env = kwargs.get("env")

    def __repr__(self):
        return "<TestEnvironment 0x%x Name: %s>" % (id(self), self.name)


class Matrix(object):
    """A class for tracking what is there in the world to be tested

    This is more or less a schedule
    """

    def __init__(self):
        self._matrix = dict()

    def __repr__(self):
        return str(self._matrix)

    def clear(self):
        self._matrix.clear()

    def find_neo(self, project, task, env):
        """Find Neo in the matrix at given location"""
        _proj = self._matrix.get(project)
        if _proj:
            _task = _proj.get(task)
            if _task:
                _env = _task.get(env)
                if _env:
                    return _env
        raise ValueError("Neo is not in the matrix")

    def send_neo(self, project, task, env, task_instance):
        """Send Neo to the given location to repair the matrix"""
        if project not in self._matrix:
            self._matrix[project] = dict()
        if task not in self._matrix[project]:
            self._matrix[project][task] = dict()
        self._matrix[project][task][env] = task_instance

    def tasks(self):
        """Find matrix glitches Neo should work on"""
        for _, project_data in self._matrix.items():
            for _, task_data in project_data.items():
                for _, env_data in task_data.items():
                    yield env_data

    def report_stats(self, statsd, zero: bool = False) -> None:
        _stats = {}
        for pname, project_data in self._matrix.items():
            _stats[pname] = {}
            for tname, task_data in project_data.items():
                for ename, env_data in task_data.items():
                    try:
                        _stats[pname][ename] += 1
                    except KeyError:
                        _stats[pname][ename] = 1
        base_name = 'apimon.scheduler.queue.{env}.{project}.cnt_tasks'
        for pname, pdata in _stats.items():
            for ename, cnt_tasks in pdata.items():
                statsd.gauge(
                    base_name, cnt_tasks if not zero else 0,
                    env=ename, project=pname
                )
