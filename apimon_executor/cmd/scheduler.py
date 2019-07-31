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

import argparse
import os

from apimon_executor import config
from apimon_executor import scheduler


class ApimonScheduler(object):

    def __init__(self):
        self.parser = argparse.ArgumentParser()

    def add_arguments(self):

        self.parser.add_argument(
            '--config',
            help='Path to the config file'
        )
        self.parser.add_argument(
            '--repo',
            help='URL of the repository to get scenarios from.'
        )
        self.parser.add_argument(
            '--location',
            help='Location in the repository to get playbooks from.',
        )
        self.parser.add_argument(
            '--git-checkout-dir',
            help='Working diretory to check the repository out.'
        )
        self.parser.add_argument(
            '--git-ref',
            help='Git reference to use.'
        )
        self.parser.add_argument(
            '--git-refresh-interval',
            type=int,
            help='Interval in seconds to check repository for updates.'
        )
        self.parser.add_argument(
            '--count-executor-threads',
            type=int,
            help='Count of the executor threads.'
        )
        self.parser.add_argument(
            '--simulate',
            type=bool,
            help='Simulate execution.'
        )

    def main(self):
        self.add_arguments()

        args = self.parser.parse_args()

        cnf = config.ExecutorConfig(args)

        cwd = os.getcwd()

        try:
            scheduler.ApimonScheduler(cnf).start()
        finally:
            os.chdir(cwd)


def main():
    ApimonScheduler().main()


if __name__ == "__main__":
    main()
