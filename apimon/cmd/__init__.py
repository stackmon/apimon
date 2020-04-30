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

import abc
import argparse
import logging
import logging.config
import os
import socket

from apimon.lib.config import Config


class App(object, metaclass=abc.ABCMeta):
    app_name = None  # type: str
    app_description = None  # type: str

    def __init__(self):
        self.args = None
        self.config = None

    def create_parser(self):
        parser = argparse.ArgumentParser(
            description=self.app_description,
            formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument(
            '--config',
            dest='config',
            help='specify the config file')
        return parser

    def parse_arguments(self, args=None):
        parser = self.create_parser()
        self.args = parser.parse_args(args)

        return parser

    @abc.abstractmethod
    def run(self):
        pass

    def read_config(self):
        self.config = Config()
        self.config.read(self.args.config)

    def setup_logging(self, section='log', parameter='config'):
        logging_config = self.config.get_default(section, parameter)

        if logging_config:
            fp = os.path.expanduser(logging_config)
            with open(fp, 'r') as f:
                logging.config.fileConfig(f)
        else:
            logging.basicConfig(level=logging.INFO)
            if hasattr(self.args, 'debug') and self.args.debug:
                logging.setDebug()

    def main(self):
        self.parse_arguments()
        self.read_config()

        self.run()

    def send_command(self, cmd):
        command_socket = self.config.get_default(
            self.app_name, 'socket',
            '/var/lib/apimon/%s.socket' % self.app_name)
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(command_socket)
        cmd = '%s\n' % cmd
        s.sendall(cmd.encode('utf8'))
