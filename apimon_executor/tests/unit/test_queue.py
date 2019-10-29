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

from unittest import TestCase

from apimon_executor import queue as _queue


class TestUniqueQueue(TestCase):

    def setUp(self):
        super(TestUniqueQueue, self).setUp()
        self.queue = _queue.UniqueQueue()

    def test_put_get(self):
        self.queue.put('a')
        self.assertEqual('a', self.queue.get())

    def test_get_nowait(self):
        self.queue.put('a')
        self.assertTrue('a' in self.queue._set)
        item = self.queue.get_nowait()
        self.assertEqual('a', item)

    def test_put_multiple_get(self):
        self.queue.put('a')
        self.queue.put('a')
        self.assertTrue('a' in self.queue._set)
        self.assertEqual('a', self.queue.get())
        self.assertTrue(self.queue.empty())

    def test_clear(self):
        self.queue.put('a')
        self.queue.put('a')
        self.queue.put('b')
        self.assertEqual(2, self.queue.qsize())
        self.queue.clear()
        self.assertTrue(self.queue.empty())

    def test_get_empty(self):
        self.queue.clear()
        self.assertRaises(_queue.Empty, self.queue.get, False, 1)
