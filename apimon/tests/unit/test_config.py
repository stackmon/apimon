
from unittest import TestCase
from unittest import mock

from apimon.lib import config


class TestConfig(TestCase):

    def setUp(self):
        self.args = mock.Mock()
        # TODO: generate config

    def test_default(self):
        cfg = config.Config()
        cfg.read('etc/apimon.yaml')

        self.assertEqual('wrk', cfg.get_default('scheduler', 'work_dir'))
