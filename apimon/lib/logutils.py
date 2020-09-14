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

import logging


def get_annotated_logger(logger, event, job=None):
    if isinstance(logger, JobTaskLogAdapter):
        extra = logger.extra
    else:
        extra = {}

    if event is not None:
        if hasattr(event, 'id'):
            extra['id'] = event.id
        else:
            extra['id'] = event

    if job is not None:
        extra['job'] = job

    if isinstance(logger, JobTaskLogAdapter):
        return logger

    return JobTaskLogAdapter(logger, extra)


class JobTaskLogAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        msg, kwargs = super().process(msg, kwargs)
        extra = kwargs.get('extra', {})
        id = extra.get('id')
        job = extra.get('job')
        new_msg = []
        if id is not None:
            new_msg.append('[unique: %s]' % id)
        if job is not None:
            new_msg.append('[job_id: %s]' % job)
        new_msg.append(msg)
        msg = ' '.join(new_msg)
        return msg, kwargs
