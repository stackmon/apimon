import copy
from pathlib import Path
import configparser
import tempfile
import unittest
import mock

from apimon_executor import scheduler as _scheduler


class ExecutorTest(unittest.TestCase):

    def test_prepare_ansible_cfg_no_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir, 'ansible.cfg')

            self.assertFalse(config_file.exists())
            executor = _scheduler.Executor(None, None, None, None)

            executor._prepare_ansible_cfg(tmpdir)

            self.assertTrue(config_file.exists())

            config = configparser.ConfigParser()

            config.read(config_file)

            self.assertEqual(config['defaults']['stdout_callback'],
                             'apimon_logger')

    def test_prepare_ansible_cfg_override_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:

            config_file = Path(tmpdir, 'ansible.cfg')

            precond = {
                'defaults': {
                    'stdout_callback': 'fake',
                    'callback_whitelist': 'a1,a2,a3'
                },
                'fake_section': {
                    'fake_key': 'fake_val'
                }
            }

            executor = _scheduler.Executor(None, None, None, None)

            config = configparser.ConfigParser()
            config.read_dict(precond)
            with open(Path(tmpdir, 'ansible.cfg'), 'w') as f:
                config.write(f)

            executor._prepare_ansible_cfg(tmpdir)

            self.assertTrue(config_file.exists())

            config = configparser.ConfigParser()

            config.read(config_file)

            actual = {s: dict(config.items(s)) for s in config.sections()}

            expected = copy.deepcopy(precond)
            expected['defaults']['stdout_callback'] = 'apimon_logger'

            self.assertDictEqual(actual, expected)


