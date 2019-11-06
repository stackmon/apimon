from pathlib import Path

from unittest import TestCase
from unittest import mock

import tempfile
import yaml

from apimon_executor import config


class TestConfig(TestCase):

    def setUp(self):
        self.args = mock.Mock()
        # TODO: generate config

    def _get_config(self, **attrs):
        config_dict = {
            'executor': {
                'test_projects': [{
                    'name': 'apimon',
                    'repo_url':
                    'https://github.com/opentelekomcloud-infra/apimon-tests',
                    'repo_ref': 'master',
                    'location': 'playbooks/scenarios',
                }],
                'work_dir': 'wrk',
                'refresh_interval': 2,
                'count_executor_threads': 8,
                'log': {
                    'jobs': {
                        'swift': {
                            'cloud_name': '9',
                            'container_name': '10',
                            'keep_seconds': 11
                        },
                        'fs': {
                            'dest': '12',
                            'archive': False,
                            'keep': True
                        }
                    }
                }
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir, 'config.yaml')
            config_file.touch()
            with open(config_file, 'w') as yaml_file:
                yaml.dump(config_dict, yaml_file, default_flow_style=False)
            self.args.config = config_file
            return config.ExecutorConfig(self.args)

    def test_default(self):
        cfg = self._get_config()

        self.assertEqual('9', cfg.log_swift_cloud)
        self.assertEqual('10', cfg.log_swift_container_name)
        self.assertEqual(11, cfg.log_swift_keep_time)
        self.assertEqual('12', cfg.log_dest)
        self.assertEqual(True, cfg.log_fs_keep)
        self.assertEqual(False, cfg.log_fs_archive)

    def test_config_no_log_config(self):
        pass

    def test_projects(self):
        cfg = self._get_config()
        self.assertEqual(1, len(cfg.projects))
        for name, cls in cfg.projects.items():
            # TODO: check for generated values
            self.assertEqual(
                cls.repo_url,
                'https://github.com/opentelekomcloud-infra/apimon-tests')
            self.assertEqual(cls.repo_ref, 'master')
        self.assertEqual(cfg.log_config,
                         '/usr/app/task_executor/etc/logging.conf')
