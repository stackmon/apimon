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

import os
import yaml


class Config(object):
    def __init__(self):
        self._fp = None

    def _find_config(self, path=None):
        if not path and self._fp:
            return self._fp

        if path:
            locations = [path]
        else:
            locations = ['/etc/apimon/apimon.yaml',
                         '~/apimon/apimon.yaml']
        for fp in locations:
            if os.path.exists(os.path.expanduser(fp)):
                self._fp = fp
                return self._fp
        raise Exception("Unable to locate config file in %s" % locations)

    def read(self, path=None):
        fp = self._find_config(path)

        with open(fp, 'r') as f:
            self.config = yaml.load(f, Loader=yaml.SafeLoader)
        return self

    def get_section(self, section):
        return self.config.get(section, {})

    def get_default(self, section, option, default=None, expand_user=False):
        if not section or not option:
            raise RuntimeError('get_default without section/option is not '
                               'possible')
        sect = self.config.get(section, {})
        if sect and option in sect:
            # Need to be ensured that we get suitable
            # type from config file by default value
            value = sect.get(option)
            if value is None:
                return default
            if isinstance(default, bool):
                value = bool(value)
            elif isinstance(default, int):
                value = int(value)
        else:
            value = default
        if expand_user and value:
            return os.path.expanduser(value)
        return value
