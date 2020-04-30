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
import threading
import traceback

import gear


class GearWorker:
    def __init__(self, name, logger_name, thread_name, config, jobs,
                 worker_class=gear.TextWorker, worker_args=None):
        self.log = logging.getLogger(logger_name)

        self._wake_event = threading.Event()

        self._running = True
        self.name = name
        self.worker_class = worker_class
        self.worker_args = worker_args if worker_args is not None else []
        self.config = config

        self.server = config.get_default('gearman', 'server')
        self.port = config.get_default('gearman', 'port', 4730)
        self.ssl_key = config.get_default('gearman', 'ssl_key')
        self.ssl_cert = config.get_default('gearman', 'ssl_cert')
        self.ssl_ca = config.get_default('gearman', 'ssl_ca')

        self.gearman = None
        self.jobs = jobs

        self.thread = threading.Thread(target=self._run, name=thread_name)
        self.thread.daemon = True

    def start(self):
        gear_args = self.worker_args + [self.name]
        self.gearman = self.worker_class(*gear_args)
        self.log.debug('Connect to gearman')
        for server in self.config.get_section('gear'):
            self.gearman.addServer(server.get('host'), server.get('port'),
                                   server.get('ssl_key'),
                                   server.get('ssl_cert'),
                                   server.get('ssl_ca'),
                                   keepalive=True, tcp_keepidle=60,
                                   tcp_keepintvl=30, tcp_keepcnt=5)
        self.log.debug('Waiting for server')
        self.gearman.waitForServer()
        self.register()
        self._accepting = True
        self.thread.start()

    def register(self):
        self.log.debug('Registering jobs')
        for job in self.jobs:
            self.gearman.registerFunction(job)

    def unregister(self):
        self.log.debug('Unregistering jobs')
        for job in self.jobs:
            self.gearman.unRegisterFunction(job)

    def stop(self):
        self._running = False
        self._wake_event.set()
        self.gearman.stopWaitingForJobs()
        self.thread.join()
        self.gearman.shutdown()

    def join(self):
        self.thread.join()

    def pause(self):
        self._accepting = False
        self.gearman.stopWaitingForJobs()
#        self.unregister()

    def resume(self):
        self._accepting = True
        self._wake_event.set()

    def _run(self):
        while self._running:
            if self._accepting:
                try:
                    job = self.gearman.getJob()
                    try:
                        if job.name not in self.jobs:
                            self.log.exception("Exception while running job")
                            job.sendWorkException(
                                traceback.format_exc().encode('utf8'))
                            continue
                        self.jobs[job.name](job)
                    except Exception:
                        self.log.exception('Exception while running job')
                        job.sendWorkException(
                            traceback.format_exc().encode('utf-8'))
                except gear.InterruptedError:
                    pass
                except Exception:
                    self.log.exception('Exception while getting job')
            else:
                if self._running:
                    self._wake_event.wait(10)
                    self._wake_event.clear()
