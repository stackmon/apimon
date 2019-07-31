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
import yaml


class ExecutorConfig(object):

    def __init__(self, args):
        config = {}

        if args.config:
            with open(args.config) as f:
                config = yaml.load(f, Loader=yaml.SafeLoader)
        else:
            config['executor'] = {
                'git_checkout_dir': 'wrk',
                'location': 'playbooks/scenarios',
                'git_ref': 'master',
                'refresh_interval': 120,
                'count_executor_threads': 5,
                'exec_cmd': 'ansible-playbook -i inventory/testing',
                'log_dest': '/var/log/executor/logs',
                'log_config': '/usr/app/task_executor/etc/logging.conf',
                'simulate': False
            }

        if 'log_config' in config['executor']:
            with open(config['executor']['log_config']) as f:
                logging.config.fileConfig(f)
        else:
            logging.basicConfig(level=logging.INFO)

        for key in ['repo', 'location', 'git_checkout_dir', 'git_ref',
                    'log_config', 'scenarios', 'log_dest',
                    'count_executor_threads', 'git_refresh_interval']:
            val = getattr(args, key, None)
            if not val:
                val = config['executor'].get(key)
            setattr(self, key, val)
        setattr(self, 'exec_cmd', 'ansible-playbook -i inventory/testing %s')
        setattr(self, 'simulate', bool(config['executor'].get('simulate')))
