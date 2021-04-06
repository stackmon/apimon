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


def merge_nested_dicts(a, b):
    """Naive merge of nested dictinaries
    """
    result = dict()
    if isinstance(a, str) and isinstance(b, str):
        return b
    elif isinstance(a, list) and isinstance(b, list):
        return [a, b]
    elif isinstance(a, dict) and isinstance(b, dict):
        for k in a.keys() | b.keys():
            if k not in a and k in b:
                result[k] = b[k]
            elif k in a and k not in b:
                result[k] = a[k]
            else:
                result[k] = merge_nested_dicts(a[k], b[k])
    else:
        raise ValueError('Cannot merge different types')

    return result


class Config(object):
    def __init__(self):
        self._fp = None
        self.config = None

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

        secure = self.config.get('secure')
        if secure and os.path.exists(os.path.expanduser(secure)):
            with open(secure, 'r') as f:
                secure_config = yaml.load(f, Loader=yaml.SafeLoader)
            # Merge secure_config into the main config
            self.config = merge_nested_dicts(self.config, secure_config)
#            self.config.update(secure_config)
#            self.config = {k: dict(self.config.get(k, {}),
#                                   **secure_config.get(k, {})) for k in
#                           self.config.keys() | secure_config.keys()}

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
