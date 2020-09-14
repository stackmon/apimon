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
import logging.config
import os
import yaml

from apimon_executor import project


class Server(object):
    def __init__(self, host, port, ssl_key, ssl_cert, ssl_ca):
        self.host = host
        self.port = port
        self.ssl_key = ssl_key
        self.ssl_cert = ssl_cert
        self.ssl_ca = ssl_ca


class Config(object):

    def __init__(self, args):
        self.args = args
        self.gear_servers = []
        self._config = {}
        self.read()

    def read(self):
        self.projects = {}

        with open(self.args.config, 'r') as f:
            global_config = yaml.load(f, Loader=yaml.SafeLoader)
            self._config = global_config
        if 'executor' in self._config:
            executor_cfg = global_config['executor']

        self.work_dir = executor_cfg.get('work_dir', 'wrk')

        for item in self._config.get('test_projects', {}):
            prj = project.Project(
                name=item.get('name'),
                repo_url=item.get('repo_url'),
                repo_ref=item.get('repo_ref'),
                project_type=item.get('type'),
                location=item.get('scenarios_location'),
                exec_cmd=item.get('exec_cmd'),
                work_dir=self.work_dir,
                env=item.get('env'),
                scenarios=item.get('scenarios')
            )
            self.projects[prj.name] = prj

        self.simulate = executor_cfg.get('simulate',
                                         getattr(self.args, 'simulate', False))
        log_config = executor_cfg.get('log', {})
        self.log_config = log_config.get(
            'config',
            '/usr/app/task_executor/etc/logging.conf')
        job_logs_config = log_config.get('jobs')
        log_fs_config = job_logs_config.get('fs', {})
        self.log_dest = log_fs_config.get(
            'dest', '/var/log/executor/logs')
        self.log_fs_archive = log_fs_config.get(
            'archive', True)
        self.log_fs_keep = log_fs_config.get(
            'keep', False)
        log_swift_config = job_logs_config.get('swift')
        self.log_swift_cloud = None
        if log_swift_config:
            self.log_swift_cloud = log_swift_config.get('cloud_name')
            self.log_swift_container_name = log_swift_config.get(
                'container_name')
            self.log_swift_keep_time = int(log_swift_config.get(
                'keep_seconds', 1209600))

        self.count_executor_threads = executor_cfg.get(
            'count_executor_threads',
            getattr(self.args, 'count_executor_threads', 5))
        self.refresh_interval = executor_cfg.get('refresh_interval', 120)

        if os.path.exists(self.log_config):
            with open(self.log_config) as f:
                logging.config.fileConfig(f)
        else:
            logging.basicConfig(level=logging.INFO)

        for gear in executor_cfg.get('gear'):
            host = gear.get('host')
            port = gear.get('port', 4730)
            ssl_key = gear.get('ssl_key')
            ssl_cert = gear.get('ssl_cert')
            ssl_ca = gear.get('ssl_ca')
            self.gear_servers.append(
                Server(host, port, ssl_key, ssl_cert, ssl_ca)
            )

        alerta = executor_cfg.get('alerta')
        if alerta:
            self.alerta_endpoint = alerta.get('endpoint')
            self.alerta_token = alerta.get('token')
            self.alerta_env = alerta.get('env', 'Production')
            self.alerta_origin = alerta.get('origin', 'task_executor')
        else:
            self.alerta_endpoint = None
            self.alerta_token = None
            self.alerta_env = None
            self.alerta_origin = None

    def get_default(self, section, var, default=None):
        print('config is %s' % self._config)
        if section not in self._config:
            print('return default')
            return default
        return self._config[section].get(var, default)
