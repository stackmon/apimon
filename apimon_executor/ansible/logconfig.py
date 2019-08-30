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
import abc
import copy
import logging
import logging.config
import json
import os
import yaml


def filter_result(result):
    """Remove keys from shell/command output.

    """

    stdout = result.pop('stdout', '')
    stdout_lines = result.pop('stdout_lines', [])
    if not stdout_lines and stdout:
        stdout_lines = stdout.split('\n')

#     for key in ('changed', 'cmd', 'invocation',
#                 'stderr', 'stderr_lines'):
#         result.pop(key, None)
    return stdout_lines


def _sanitize_filename(name):
    return ''.join(c for c in name if c.isalnum())


_DEFAULT_JOB_LOGGING_CONFIG = {
    'version': 1,
    'formatters': {
        'plain': {'format': '%(message)s'},
        'result': {'format': 'RESULT %(message)s'},
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'level': 'INFO',
            'formatter': 'plain',
        },
        'result': {
            # result is returned to subprocess stdout and is used to pass
            # control information from the callback back to the executor
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'level': 'INFO',
            'formatter': 'result',
        },
        'jobfile': {
            # used by executor to emit log file
            'class': 'logging.FileHandler',
            'level': 'INFO',
            'formatter': 'plain',
            'filename': 'job-output.log'
        },
    },
    'loggers': {
        'apimon_executor.ansible.result': {
            'handlers': ['result'],
            'level': 'INFO',
        },
        'apimon_executor.ansible': {
            'handlers': ['jobfile'],
            'level': 'INFO',
        },
    },
    'root': {'handlers': []},
}


def _read_config_file(filename: str):
    if not os.path.exists(filename):
        raise ValueError("Unable to read logging config file at %s" % filename)

    if os.path.splitext(filename)[1] in ('.yml', '.yaml', '.json'):
        return yaml.safe_load(open(filename, 'r'))
    return filename


def load_config(filename):
    config = _read_config_file(filename)
    if isinstance(config, dict):
        return DictLoggingConfig(config)
    return FileLoggingConfig(filename)


def load_job_config(filename: str):
    return JobLoggingConfig(_read_config_file(filename))


class LoggingConfig(object, metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def apply(self):
        """Apply the config information to the current logging config."""


class DictLoggingConfig(LoggingConfig, metaclass=abc.ABCMeta):

    def __init__(self, config):
        self._config = config

    def apply(self):
        logging.config.dictConfig(self._config)

    def writeJson(self, filename: str):
        with open(filename, 'w') as f:
            f.write(json.dumps(self._config, indent=2))


class JobLoggingConfig(DictLoggingConfig):

    def __init__(self, config=None, job_output_file=None):
        if not config:
            config = copy.deepcopy(_DEFAULT_JOB_LOGGING_CONFIG)

        super(JobLoggingConfig, self).__init__(config=config)
        if job_output_file:
            self.job_output_file = job_output_file

    def setDebug(self):
        self._config['loggers']['executor.ansible']['level'] = 'DEBUG'

    @property
    def job_output_file(self):
        return self._config['handlers']['jobfile']['filename']

    @job_output_file.setter
    def job_output_file(self, filename):
        self._config['handlers']['jobfile']['filename'] = filename


class FileLoggingConfig(LoggingConfig):

    def __init__(self, filename):
        self._filename = filename

    def apply(self):
        logging.config.fileConfig(self._filename)
