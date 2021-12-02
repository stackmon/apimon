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

from hvac import exceptions as hvac_exceptions


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


def expand_dict_vars(value, vault_client=None):
    """Expand config dict with values from vault"""
    res = dict()
    for k, v in value.items():
        if isinstance(v, dict):
            res[k] = expand_dict_vars(v, vault_client)
        else:
            res[k] = expand_vars(v, vault_client)
    return res


def expand_vars(value, vault_client=None):
    """Expand value following reference to vault"""
    if not isinstance(value, str):
        return value
    if not value.startswith('vault|') or not vault_client:
        return value
    try:
        pairs = value.split('|')
        dt = {k: v for k, v in [x.split(':') for x in pairs[1:]]}
        if dt['engine'] == 'secret':
            data = vault_client.secrets.kv.v2.read_secret(
                path=dt['path']
            )['data']['data']
            if dt['attr'] in data:
                return data.get(dt['attr'])
            else:
                logging.error(
                    f"Attribute {dt['attr']} is not present on "
                    f"{dt['path']}"
                )
    except hvac_exceptions.InvalidPath:
        logging.error(f"Cannot find secret {dt['path']} in vault")
        return None
