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

import logging
import signal
import sys

import apimon.cmd
import apimon.executor.server


class ApimonExecutor(apimon.cmd.App):
    app_name = 'executor'
    app_description = 'APImon Executor process'

    def __init__(self):
        super(ApimonExecutor, self).__init__()

    def create_parser(self):
        parser = super(ApimonExecutor, self).create_parser()

        parser.add_argument(
            '--zone',
            dest='zone',
            help='Executor zone'
        )

        parser.add_argument(
            'command',
            nargs='?',
            choices=apimon.executor.server.COMMANDS,
            help='command'
        )

        return parser

    def exit_handler(self, signum, frame) -> None:
        self.executor.stop()
        self.executor.join()
        sys.exit(0)

    def reconfigure_handler(self, signum, frame) -> None:
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        self.reconfigure()
        signal.signal(signal.SIGHUP, self.reconfigure_handler)

    def pause_handler(self, signum, frame) -> None:
        self.executor.pause()

    def resume_handler(self, signum, frame) -> None:
        self.executor.resume()

    def reconfigure(self):
        self.log.debug('Reconfiguration triggered')
        self.read_config()
        self.setup_logging()
        try:
            self.executor.reconfigure(self.config)
        except Exception:
            self.log.exception('Reconfiguration failed')

    def run(self):
        if self.args.command in apimon.executor.server.COMMANDS:
            self.send_command(self.args.command)
            sys.exit(0)

        self.read_config()
        self.setup_logging()

        self.log = logging.getLogger("apimon.executor")

        self.log.info('Starting...')

        self.executor = apimon.executor.server.ExecutorServer(
            config=self.config, zone=self.args.zone)
        self.executor.start()

        signal.signal(signal.SIGHUP, self.reconfigure_handler)
        signal.signal(signal.SIGTERM, self.exit_handler)
        signal.signal(signal.SIGUSR1, self.pause_handler)
        signal.signal(signal.SIGUSR2, self.resume_handler)
        while True:
            try:
                signal.pause()
            except KeyboardInterrupt:
                print("Ctrl + C: asking apimon executor to exit nicely...\n")
                self.exit_handler(signal.SIGINT, None)


def main():
    ApimonExecutor().main()


if __name__ == "__main__":
    main()
