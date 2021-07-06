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

import datetime
import unittest
import uuid
import time

from apimon.lib import config as _config
from apimon.executor import message
from apimon.executor import resultprocessor


class TestResultProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = _config.Config()
        cls.config.read('etc/apimon.yaml')
        cls.processor = resultprocessor.ResultProcessor(
            cls.config
        )
        cls.processor.start()

    @classmethod
    def tearDownClass(cls):
        cls.processor.stop()
        cls.processor.join()

    def test_task(self):
        if not self.processor.db_conn.connected:
            self.skipTest('DB not available for test')

        task = message.ResultTask(
            name=uuid.uuid4().hex, result=1, duration=2,
            environment=uuid.uuid4().hex,
            zone=uuid.uuid4().hex,
            job_id=uuid.uuid4().hex
        )
        summ = message.ResultSummary(
            name=uuid.uuid4().hex, result=1, duration=2,
            job_id=uuid.uuid4().hex,
            timestamp=datetime.datetime.now().isoformat(),
            environment=uuid.uuid4().hex,
            zone=uuid.uuid4().hex
        )

        self.processor.add_entry(task)
        self.processor.add_entry(summ)
        time.sleep(1)
        with self.processor.db_conn.get_session() as sess:
            rt = sess.get_result_task(task['job_id'], task['name'])
            self.assertEqual(rt.duration, task['duration'])

            rs = sess.get_result_summary(summ['job_id'], summ['name'])
            self.assertEqual(rs.environment, summ['environment'])
            sess.session().delete(rt)
            sess.session().delete(rs)

    def test_job(self):
        if not self.processor.db_conn.connected:
            self.skipTest('DB not available for test')

        job = dict(
            job_id=uuid.uuid4().hex,
            name=uuid.uuid4().hex, result=1, duration=2,
            environment=uuid.uuid4().hex,
            zone=uuid.uuid4().hex,
            log_url=uuid.uuid4().hex
        )

        self.processor.add_job_entry(job)
        time.sleep(1)
        with self.processor.db_conn.get_session() as sess:
            je = sess.get_job(job['job_id'])
            self.assertEqual(je.duration, job['duration'])

            sess.session().delete(je)
