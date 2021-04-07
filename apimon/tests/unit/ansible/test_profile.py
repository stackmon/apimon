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
import json

import unittest

from apimon.ansible import profile


class TestProfile(unittest.TestCase):
    def test_task(self):
        prof = profile.ProfileTask(
            'name', 1, 2, 'job_id',
            'act', 'play', 'long_name', 'state', 'az',
            'raw_rsp', 'an_rsp')
        self.assertEqual(prof['name'], 'name')
        self.assertEqual(prof['result'], 1)
        self.assertEqual(prof['duration'], 2)
        self.assertEqual(prof['action'], 'act')
        self.assertEqual(prof['play'], 'play')
        self.assertEqual(prof['long_name'], 'long_name')
        self.assertEqual(prof['state'], 'state')
        self.assertEqual(prof['az'], 'az')
        self.assertEqual(prof['raw_response'], 'raw_rsp')
        self.assertEqual(prof['anonymized_response'], 'an_rsp')

    def test_summary(self):
        prof = profile.ProfileSummary(
            'name', 1, 2, 'job_id',
            3, 4, 5, 6)
        self.assertEqual(prof['name'], 'name')
        self.assertEqual(prof['result'], 1)
        self.assertEqual(prof['duration'], 2)
        self.assertEqual(prof['count_passed'], 3)
        self.assertEqual(prof['count_skipped'], 4)
        self.assertEqual(prof['count_failed'], 5)
        self.assertEqual(prof['count_ignored'], 6)

    def test_metric(self):
        prof = profile.Metric(
            'name', 1, 2)
        self.assertEqual(prof['name'], 'name')
        self.assertEqual(prof['result'], 1)
        self.assertEqual(prof['duration'], 2)

    def test_metric_kwargs(self):
        prof = profile.Metric(
            'name', 1, 2, az='a')
        self.assertEqual(prof['name'], 'name')
        self.assertEqual(prof['result'], 1)
        self.assertEqual(prof['duration'], 2)
        self.assertEqual(prof['az'], 'a')

    def test_serialize(self):
        prof = profile.ProfileTask(
            'name', 1, 2, 'job_id',
            'act', 'play', 'long_name', 'state', 'az',
            'raw_rsp', 'an_rsp')
        msg = prof.serialize()
        self.assertIsNotNone(msg)
        compare = json.loads(msg)
        self.assertDictEqual(prof, compare)
