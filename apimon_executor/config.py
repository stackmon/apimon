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


class ExecutorConfig(object):

    def __init__(self, args):
        self.projects = {}

        with open(args.config, 'r') as f:
            global_config = yaml.load(f, Loader=yaml.SafeLoader)
        if 'executor' in global_config:
            executor_cfg = global_config['executor']

        self.work_dir = executor_cfg.get('work_dir', 'wrk')

        for item in executor_cfg.get('test_projects', {}):
            prj = project.Project(
                name=item.get('name'),
                repo_url=item.get('repo_url'),
                repo_ref=item.get('repo_ref', 'master'),
                project_type=item.get('type', 'ansible'),
                location=item.get('scenarios_location', 'playbooks'),
                exec_cmd=item.get('exec_cmd', 'ansible-playbook -i '
                                  'inventory/testing %s'),
                work_dir=self.work_dir,
                env=item.get('env'),
                scenarios=item.get('scenarios')
            )
            self.projects[prj.name] = prj

        self.simulate = executor_cfg.get('simulate',
                                         getattr(args, 'simulate', False))
        log_config = executor_cfg.get('log', {})
        self.log_config = log_config.get(
            'log_config',
            '/usr/app/task_executor/etc/logging.conf')
        log_fs_config = log_config.get('fs', {})
        self.log_dest = log_fs_config.get(
            'dest', '/var/log/executor/logs')
        self.log_fs_archive = log_fs_config.get(
            'archive', True)
        self.log_fs_keep = log_fs_config.get(
            'keep', False)
        log_swift_config = log_config.get('swift')
        self.log_swift_cloud = None
        if log_swift_config:
            self.log_swift_cloud = log_swift_config.get('cloud_name')
            self.log_swift_container_name = log_swift_config.get(
                'container_name')
            self.log_swift_keep_time = int(log_swift_config.get(
                'keep_seconds', 1209600))

        self.count_executor_threads = executor_cfg.get(
            'count_executor_threads',
            getattr(args, 'count_executor_threads', 5))
        self.refresh_interval = executor_cfg.get('refresh_interval', 120)

        if os.path.exists(self.log_config):
            with open(self.log_config) as f:
                logging.config.fileConfig(f)
        else:
            logging.basicConfig(level=logging.INFO)
