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

import threading
import queue


class Empty(queue.Empty):
    """A dummy override of the original Empty exception
    to be able to re-reraise and catch with no changes
    """
    pass


class UniqueQueue(queue.Queue):
    """Queue, where items are uniqie
    """

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self._set = set()

    def get_item_key(self, item):
        """Try to get a key for the item
        """
        if hasattr(item, 'name'):
            return item.name
        else:
            return item

    def put(self, item, block=True, timeout=None):
        with self._lock:
            key = self.get_item_key(item)
            if key in self._set:
                return
            else:
                super(UniqueQueue, self).put(item, block, timeout)
                self._set.add(key)

    def get(self, block=True, timeout=None):
        try:
            item = super(UniqueQueue, self).get(block, timeout)
            with self._lock:
                self._set.discard(self.get_item_key(item))
            return item
        except queue.Empty as e:
            raise Empty(e)

    def clear(self):
        with self._lock:
            while not self.empty():
                super(UniqueQueue, self).get(False)
                self.task_done()
            self._set.clear()
