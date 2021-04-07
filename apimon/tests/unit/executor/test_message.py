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

import unittest

from apimon.executor import message


class TestMessage(unittest.TestCase):
    def test_deserialize_metric(self):
        self.assertIsInstance(
            message.get_message(
                {
                    "__type": "metric",
                    "name": "a",
                    "value": 2,
                    "environment": "b",
                    "zone": "c",
                    "metric_type": "c"
                }
            ),
            message.Metric,
        )

    def test_deserialize_profile_task(self):
        self.assertIsInstance(
            message.get_message(
                {
                    "__type": "ansible_profile_task",
                    "name": "a",
                    "duration": 1,
                    "result": 2,
                    "job_id": "sdf",
                    "environment": "b",
                    "zone": "c",
                }
            ),
            message.ResultTask,
        )

    def test_deserialize_profile_summary(self):
        self.assertIsInstance(
            message.get_message(
                {
                    "__type": "ansible_profile_summary",
                    "name": "a",
                    "duration": 1,
                    "result": 2,
                    "job_id": "b",
                    "environment": "b",
                    "zone": "c",
                }
            ),
            message.ResultSummary,
        )

    def test_timestamp(self):
        obj = message.get_message(
            {
                "__type": "metric",
                "name": "a",
                "value": 1,
                "metric_type": "c"
            }
        )
        self.assertIsNotNone(obj["timestamp"])

    def test_serialize(self):
        obj = message.get_message(
            {
                "__type": "metric",
                "name": "a",
                "value": 1,
                "environment": "b",
                "zone": "c",
                "metric_type": "c"
            }
        )
        ser_data = obj.serialize()
        ts = obj["timestamp"]
        self.assertEqual(
            (
                '{"name":"a","environment":"b",'
                '"zone":"c","timestamp":"' + ts + '",'
                '"__type":"metric","metric_type":"c","value":1}'
            ),
            ser_data,
        )

    def test_get_metric_data(self):
        data = (
            '{"name":"a","value":1,"environment":"b",'
            '"zone":"c",'
            '"__type":"metric","metric_type":"c"}'
        )
        obj = message.get_message(data.encode("utf8"))
        self.assertIsInstance(obj, message.Metric)
        self.assertEqual("a", obj["name"])
