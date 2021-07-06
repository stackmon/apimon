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
import queue
import threading

from sqlalchemy import exc

from apimon.executor import message
from apimon.lib import database


class ResultProcessor(threading.Thread):
    """Results processor"""

    log = logging.getLogger("apimon.executor.ResultProcessor")

    def __init__(self, config=None):
        threading.Thread.__init__(self)

        self.log.debug('Initializing executor resultprocessor')

        self._stopped = False
        self.queue = queue.Queue()
        if config:
            self.config = config
            self.db_url = config.get_default("executor", "db_url", None)
            if self.db_url:
                self._connect_to_db()
        self.wake_event = threading.Event()

    # def start(self):
    #    self.running = True

    def stop(self) -> None:
        # First, wake up our listener thread with a connection and
        # tell it to stop running.
        self._stopped = True
        self.wake_event.set()

    def _connect_to_db(self):
        self.db_conn = database.SQLConnection(self.db_url)

    def add_entry(self, entry) -> None:
        if isinstance(
            entry, (message.ResultTask, message.ResultSummary)
        ):
            self.queue.put(entry)
            self.wake_event.set()

    def run(self) -> None:
        while True:
            self.wake_event.wait(
                self.config.get_default("stat", "interval", 5))
            self.wake_event.clear()

            if self._stopped:
                return

            tasks = []
            summaries = []

            while True:
                try:
                    entry = self.queue.get(False)

                    if isinstance(entry, message.ResultTask):
                        tasks.append(entry)
                    elif isinstance(entry, message.ResultSummary):
                        summaries.append(entry)
                    self.queue.task_done()
                except queue.Empty:
                    break

            self._write_data_to_db(tasks, summaries)

    def _write_data_to_db(
        self, tasks: list, summaries: list,
        is_retry: bool = False
    ) -> None:
        try:
            with self.db_conn.get_session() as session:
                for task in tasks:
                    session.create_result_task(**task)
                for summary in summaries:
                    session.create_result_summary(**summary)
                try:
                    session.flush()
                except exc.OperationalError as ex:
                    self.log.exception('Exception during writing to DB')
                    session.rollback()
                    if ex.connection_invalidated:
                        self.log.error(
                            'Lost DB Connection identified. Reconnecting...')
                        self._connect_to_db()
                    if not is_retry:
                        # we were not retrying so far. Try one last time
                        self.log.info('Going to retry saving data')
                        self._write_data_to_db(tasks, summaries, is_retry=True)
        except exc.SQLAlchemyError:
            self.log.exception('Exception during writing to database')

    def add_job_entry(self, job, is_retry: bool=False) -> None:
        """Add job entry into DB"""

        try:
            with self.db_conn.get_session() as session:
                session.create_job_entry(**job)
                try:
                    session.flush()
                except exc.OperationalError as ex:
                    self.log.exception('Exception during writing to DB')
                    session.rollback()
                    if ex.connection_invalidated:
                        self.log.error(
                            'Lost DB Connection identified. Reconnecting...')
                        self._connect_to_db()
                    if not is_retry:
                        # we were not retrying so far. Try one last time
                        self.log.info('Going to retry saving data')
                        self.add_job_entry(job, is_retry=True)
        except exc.SQLAlchemyError:
            self.log.exception('Exception during writing to database')
