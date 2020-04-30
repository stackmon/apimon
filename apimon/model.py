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


class JobTask(object):
    def __init__(self, task_id, project, task, environment):
        self.id = task_id
        self.project = project
        self.task = task
        self.env = environment
        self.started = False
        self.job_id = None

    def __repr__(self):
        return ('<ApimonTask 0x%x ID: %s Project: %s Task: %s>' %
                (id(self), self.id, self.project.name, self.task))

    def get_job_data(self, config, config_ver=None):
        data = {
            'project': {
                'name': self.project.name,
                'url': self.project.repo_url,
                'ref': self.project.repo_ref,
                'commit': str(self.project.get_commit()),
                'task': self.task,
                'exec_cmd': self.project.exec_cmd
            },
            'env': {
                'name': self.env.name
            }
        }

        env = self.env.env
        if env:
            data['env']['vars'] = env

        if self.job_id:
            data['job_id'] = self.job_id

        if config_ver:
            data['config_version'] = config_ver

        return data


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
        if 'env' in kwargs:
            self.env = kwargs.get('env')
#        self.clouds = kwargs.get('clouds')
#        influx_conf = config.get_default('metrics', 'influxdb', {})
#        creds = {'clouds': {}}
#        for cl in kwargs.get('clouds'):
#            cloud_conf = clouds_config.get(cl)
#            if not cloud_conf:
#                raise RuntimeError('Can not find cloud %s' % cl)
#            creds['clouds'][cl] = cloud_conf.data
#        self.clouds_content = {
#            'clouds': creds
#        }
#        if influx_conf:
#            self.clouds_content['metrics'] = {
#                'influxdb': influx_conf
#            }

    def __repr__(self):
        return ('<TestEnvironment 0x%x Name: %s>' %
                (id(self), self.name))


class Matrix(object):
    def __init__(self):
        self._matrix = dict()

    def __repr__(self):
        return str(self._matrix)

    def find_neo(self, project, task, env):
        """Find Neo in the matrix at given location"""
        _proj = self._matrix.get(project)
        if _proj:
            _task = _proj.get(task)
            if _task:
                _env = _task.get(env)
                if _env:
                    return _env
        raise ValueError('Neo is not in the matrix')

    def send_neo(self, project, task, env, job_id=''):
        """Send Neo to the given location to repair the matrix"""
        if project not in self._matrix:
            self._matrix[project] = dict()
        if task not in self._matrix[project]:
            self._matrix[project][task] = dict()
        self._matrix[project][task][env] = job_id

    def find_glitches(self):
        """Find matrix glitches Neo should repair"""
        for prj_name, project in self._matrix.items():
            for task_name, task in project.items():
                for env_name, env in task.items():
                    if not env:
                        yield (prj_name, task_name, env_name)
