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

from apimon.scheduler import scheduler
from apimon.executor import client


class ApimonScheduler(apimon.cmd.App):
    app_name = 'scheduler'
    app_description = 'Main APImon process'

    def __init__(self):
        super(ApimonScheduler, self).__init__()

    def create_parser(self):
        parser = super(ApimonScheduler, self).create_parser()

        parser.add_argument(
            'command',
            nargs='?',
            choices=scheduler.COMMANDS,
            help='command'
        )

        return parser

    def exit_handler(self, signum, frame):
        self.scheduler.stop()
        self.scheduler.join()
        sys.exit(0)

    def reconfigure_handler(self, signum, frame):
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        self.reconfigure()
        signal.signal(signal.SIGHUP, self.reconfigure_handler)

    def pause_handler(self, signum, frame):
        self.scheduler.pause()

    def resume_handler(self, signum, frame):
        self.scheduler.resume()

    def reconfigure(self):
        self.log.debug('Reconfiguration triggered')
        self.read_config()
        self.setup_logging()
        try:
            self.scheduler.reconfigure(self.config)
        except Exception:
            self.log.exception('Reconfiguration failed')

    def run(self):
        if self.args.command in scheduler.COMMANDS:
            self.send_command(self.args.command)
            sys.exit(0)

        self.read_config()
        self.setup_logging()

        self.log = logging.getLogger("apimon.scheduler")

        self.scheduler = scheduler.Scheduler(self.config)

        gearman = client.JobExecutorClient(self.config, self.scheduler)

        self.scheduler.set_executor(gearman)

        try:
            self.scheduler.start()
            self.scheduler.reconfigure(self.config)
            self.scheduler.wake_up()
        except Exception:
            self.log.exception('Error starting scheduler:')
            self.scheduler.stop()
            sys.exit(1)
        finally:
            pass
            # os.chdir(cwd)

        signal.signal(signal.SIGHUP, self.reconfigure_handler)
        signal.signal(signal.SIGTERM, self.exit_handler)
        signal.signal(signal.SIGUSR1, self.pause_handler)
        signal.signal(signal.SIGUSR2, self.resume_handler)
        while True:
            try:
                signal.pause()
            except KeyboardInterrupt:
                print("Ctrl + C: asking apimon scheduler to exit nicely...\n")
                self.exit_handler(signal.SIGINT, None)


def main():
    ApimonScheduler().main()


if __name__ == "__main__":
    main()
