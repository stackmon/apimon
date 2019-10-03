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
import sys

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

        sys.exit(0)


def main():
    ApimonScheduler().main()


if __name__ == "__main__":
    main()
