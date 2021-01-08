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

import datetime
import json


class Base(dict):
    """Base metric class"""

    def __init__(
        self, name: str,
        environment: str, zone: str,
        timestamp: str = None
    ):
        super(Base, self).__init__()
        self['name'] = name
        self['environment'] = environment
        self['zone'] = zone
        if timestamp:
            self['timestamp'] = timestamp
        else:
            self['timestamp'] = datetime.datetime.now().isoformat()

    def serialize(self) -> str:
        """Serialize data as json string"""
        try:
            return json.dumps(self, separators=(',', ':'))
        except json.JSONDecodeError:
            return None

    def __bytes__(self) -> bytes:
        """Returns bytes interpretation of data"""
        data = self.serialize()
        return ('%s\n' % data).encode('utf8')


class ResultTask(Base):
    """Individual task results"""

    def __init__(
        self,
        name: str, result: int, duration: int, job_id: str = None,
        environment: str = None, zone: str = None,
        timestamp: str = None,
        action: str = None, play: str = None, long_name: str = None,
        state: str = None, az: str = None, raw_response: str = None,
        anonymized_response: str = None,
        service: str = None
    ):
        super(ResultTask, self).__init__(
            name=name,
            environment=environment,
            zone=zone,
            timestamp=timestamp)
        self['__type'] = 'ansible_profile_task'
        self['result'] = int(result)
        self['duration'] = int(duration)
        self['job_id'] = job_id
        self['action'] = action
        self['play'] = play
        self['long_name'] = long_name
        self['state'] = state
        self['az'] = az
        self['raw_response'] = raw_response
        self['anonymized_response'] = anonymized_response
        if service:
            self['service'] = service


class ResultSummary(Base):

    def __init__(
        self,
        name: str, result: int, duration: int, job_id: str = None,
        environment: str = None, zone: str = None,
        timestamp: str = None,
        count_passed: int = 0, count_skipped: int = 0,
        count_failed: int = 0, count_ignored: int = 0
    ):
        super(ResultSummary, self).__init__(
            name=name,
            environment=environment,
            zone=zone,
            timestamp=timestamp)
        self['__type'] = 'ansible_profile_summary'
        self['result'] = int(result)
        self['duration'] = int(duration)
        self['job_id'] = job_id
        self['count_passed'] = int(count_passed)
        self['count_failed'] = int(count_failed)
        self['count_skipped'] = int(count_skipped)
        self['count_ignored'] = int(count_ignored)


class Metric(Base):
    """Base metric"""

    def __init__(
        self,
        name: str, value: int,
        metric_type: str,
        environment: str = None, zone: str = None,
        **kwargs: dict
    ):
        super(Metric, self).__init__(
            name=name,
            environment=environment,
            zone=zone,
        )
        self['__type'] = 'metric'
        self['metric_type'] = metric_type
        self['value'] = value
        self.update(**kwargs)


def get_message(msg: dict):
    """Get metric instance from dictionary or string"""
    if not isinstance(msg, dict):
        try:
            msg = json.loads(msg.decode('utf8'))
        except json.JSONDecodeError:
            return None
    typ = msg.pop('__type')
    if typ == 'metric':
        return Metric(**msg)
    if typ == 'ansible_profile_task':
        return ResultTask(**msg)
    if typ == 'ansible_profile_summary':
        return ResultSummary(**msg)
