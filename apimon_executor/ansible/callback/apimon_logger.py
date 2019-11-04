# Copyright 2017 Red Hat, Inc.
#
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
# This file is based from Zuul, thus keeping license and copyright

import datetime
import logging
import logging.config
import json
import os
import threading
import time


from ansible.plugins.callback import CallbackBase

from apimon_executor.ansible import logconfig


class CallbackModule(CallbackBase):

    '''
    This is the TaskExecutor logging callback.
    '''

    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'stdout'
    CALLBACK_NAME = 'apimon_logger'

    def __init__(self):

        super(CallbackModule, self).__init__()
        self._task = None
        self._daemon_running = False
        self._play = None
        self._streamers = []
        self._streamers_stop = False
        self.configure_logger()
        self._items_done = False
        self._deferred_result = None
        self._playbook_name = None

    def configure_logger(self):
        log_config = os.getenv('APIMON_EXECUTOR_JOB_CONFIG')
        if log_config:
            logging_config = logconfig.load_job_config(log_config)
        else:
            logging_config = logconfig.JobLoggingConfig(
                job_output_file='stdout.log')

        if self._display.verbosity > 2:
            logging_config.setDebug()

        logging_config.apply()

        self._logger = logging.getLogger('apimon_executor.ansible')

    def _log(self, msg, ts=None, job=True, executor=False, debug=False):
        msg = msg.rstrip()
        if job:
            now = ts or datetime.datetime.now()
            self._logger.info("{now} | {msg}".format(now=now, msg=msg))
        if executor:
            if debug:
                self._display.vvv(msg)
            else:
                self._display.display(msg)

    def v2_playbook_on_start(self, playbook):
        self._playbook_name = os.path.splitext(playbook._file_name)[0]

        msg = u"PLAYBOOK [{name}]".format(name=self._playbook_name)

        self._log(msg)

    def v2_playbook_on_include(self, included_file):
        for host in included_file._hosts:
            self._log("{host} | included: {filename}".format(
                host=host.name,
                filename=included_file._filename))

    def v2_playbook_on_play_start(self, play):
        self._play = play
        # Log an extra blank line to get space before each play
        self._log("")

        # the name of a play defaults to the hosts string
        name = play.get_name().strip()
        msg = u"PLAY [{name}]".format(name=name)

        self._log(msg)

    def v2_playbook_on_task_start(self, task, is_conditional):
        # Log an extra blank line to get space before each task
        self._log("")

        self._task = task

        if self._play.strategy != 'free':
            task_name = self._print_task_banner(task)
        else:
            task_name = task.get_name().strip()

        hosts = self._get_task_hosts(task)

        if task.action in ('command', 'shell'):
            play_vars = self._play._variable_manager._hostvars

            hosts = self._get_task_hosts(task)
            for host, inventory_hostname in hosts:
                if host in ('localhost', '127.0.0.1'):
                    # Don't try to stream from localhost
                    continue
                ip = play_vars[host].get(
                    'ansible_host', play_vars[host].get(
                        'ansible_inventory_host'))
                if ip in ('localhost', '127.0.0.1'):
                    # Don't try to stream from localhost
                    continue
                if task.loop:
                    # Don't try to stream from loops
                    continue

                log_id = "%s-%s" % (task._uuid, ''.join(c for c in
                                                        inventory_hostname if
                                                        c.isalnum()))
                streamer = threading.Thread(
                    target=self._read_log, args=(
                        host, ip, log_id, task_name, hosts))
                streamer.daemon = True
                streamer.start()
                self._streamers.append(streamer)

    def v2_playbook_on_handler_task_start(self, task):
        self.v2_playbook_on_task_start(task, False)

    def _stop_streamers(self):
        self._streamers_stop_ts = time.monotonic()
        self._streamers_stop = True
        while True:
            if not self._streamers:
                break
            streamer = self._streamers.pop()
            streamer.join(30)
            if streamer.is_alive():
                msg = "[Zuul] Log Stream did not terminate"
                self._log(msg, job=True, executor=True)
        self._streamers_stop = False

    def _process_result_for_localhost(self, result, is_task=True):
        result_dict = dict(result._result)
        localhost_names = ('localhost', '127.0.0.1', '::1')
        is_localhost = False
        task_host = result._host.get_name()
        delegated_vars = result_dict.get('_ansible_delegated_vars', None)
        if delegated_vars:
            delegated_host = delegated_vars['ansible_host']
            if delegated_host in localhost_names:
                is_localhost = True
        elif result._task._variable_manager is None:
            # Handle fact gathering which doens't have a variable manager
            if task_host == 'localhost':
                is_localhost = True
        else:
            task_hostvars = result._task._variable_manager._hostvars[task_host]
            # Normally hosts in the inventory will have ansible_host
            # or ansible_inventory host defined.  The implied
            # inventory record for 'localhost' will have neither, so
            # default to that if none are supplied.
            if task_hostvars.get('ansible_host', task_hostvars.get(
                    'ansible_inventory_host', 'localhost')) in localhost_names:
                is_localhost = True

        if not is_localhost and is_task:
            self._stop_streamers()

    def v2_runner_on_failed(self, result, ignore_errors=False):
        result_dict = dict(result._result)

        self._handle_exception(result_dict)

        if result_dict.get('msg') == 'All items completed':
            result_dict['status'] = 'ERROR'
            self._deferred_result = result_dict
            return

        self._process_result_for_localhost(result)

        if result._task.loop and 'results' in result_dict:
            # items have their own events
            pass
        elif result_dict.get('msg', '').startswith('MODULE FAILURE'):
            self._log_module_failure(result, result_dict)
        else:
            self._log_message(
                result=result, status='ERROR', result_dict=result_dict)
        if ignore_errors:
            self._log_message(result, "Ignoring Errors", status="ERROR")

    def v2_runner_on_skipped(self, result):
        if result._task.loop:
            self._items_done = False
            self._deferred_result = dict(result._result)
        else:
            reason = result._result.get('skip_reason')
            if reason:
                # No reason means it's an item, which we'll log differently
                self._log_message(result, status='skipping', msg=reason)

    def v2_runner_item_on_skipped(self, result):
        reason = result._result.get('skip_reason')
        if reason:
            self._log_message(result, status='skipping', msg=reason)
        else:
            self._log_message(result, status='skipping')

        if self._deferred_result:
            self._process_deferred(result)

    def v2_runner_on_ok(self, result):
        if (self._play.strategy == 'free'
                and self._last_task_banner != result._task._uuid):
            self._print_task_banner(result._task)

        result_dict = dict(result._result)

        self._clean_results(result_dict, result._task.action)

        if result_dict.get('changed', False):
            status = 'changed'
        else:
            status = 'ok'

        if (result_dict.get('msg') == 'All items completed'
                and not self._items_done):
            result_dict['status'] = status
            self._deferred_result = result_dict
            return

        if not result._task.loop:
            self._process_result_for_localhost(result)
        else:
            self._items_done = False

        self._handle_warnings(result_dict)

        if result._task.loop and 'results' in result_dict:
            # items have their own events
            pass

        elif result_dict.get('msg', '').startswith('MODULE FAILURE'):
            self._log_module_failure(result, result_dict)
        elif result._task.action == 'debug':
            # this is a debug statement, handle it special
            for key in [k for k in result_dict
                        if k.startswith('_ansible')]:
                del result_dict[key]
            if 'changed' in result_dict:
                del result_dict['changed']
            keyname = next(iter(result_dict.keys()))
            # If it has msg, that means it was like:
            #
            #  debug:
            #    msg: Some debug text the user was looking for
            #
            # So we log it with self._log to get just the raw string the
            # user provided. Note that msg may be a multi line block quote
            # so we handle that here as well.
            if keyname == 'msg':
                msg_lines = result_dict['msg'].rstrip().split('\n')
                for msg_line in msg_lines:
                    self._log(msg=msg_line)
            else:
                self._log_message(
                    msg=json.dumps(result_dict, indent=2, sort_keys=True),
                    status=status, result=result)
        elif result._task.action not in ('command', 'shell'):
            if 'msg' in result_dict:
                self._log_message(msg=result_dict['msg'],
                                  result=result, status=status)
            else:
                self._log_message(
                    result=result,
                    status=status)
        elif 'results' in result_dict:
            for res in result_dict['results']:
                if 'delta' not in res:
                    res['delta'] = -1
                self._log_message(
                    result,
                    "Runtime: {delta}".format(**res))
        elif result_dict.get('msg') == 'All items completed':
            self._log_message(result, result_dict['msg'])
        else:
            if 'delta' not in result_dict:
                result_dict['delta'] = -1
            self._log_message(
                result,
                "Runtime: {delta}".format(
                    **result_dict))

    def v2_runner_item_on_ok(self, result):
        result_dict = dict(result._result)
        self._process_result_for_localhost(result, is_task=False)

        if result_dict.get('changed', False):
            status = 'changed'
        else:
            status = 'ok'

        if result_dict.get('msg', '').startswith('MODULE FAILURE'):
            self._log_module_failure(result, result_dict)
        elif result._task.action not in ('command', 'shell'):
            if 'msg' in result_dict:
                self._log_message(
                    result=result, msg=result_dict['msg'], status=status)
            else:
                self._log_message(
                    result=result,
                    msg=json.dumps(result_dict,
                                   indent=2, sort_keys=True),
                    status=status)
        else:
            stdout_lines = logconfig.filter_result(result_dict)
            for line in stdout_lines:
                hostname = self._get_hostname(result)
                self._log("%s | %s " % (hostname, line))
            if 'delta' not in result_dict:
                result_dict['delta'] = -1

            if 'item' in result_dict and isinstance(result_dict['item'], str):
                self._log_message(
                    result,
                    "Item: {item} Runtime: {delta}".format(**result_dict))
            else:
                self._log_message(
                    result,
                    "Item: Runtime: {delta}".format(
                        **result_dict))

        if self._deferred_result:
            self._process_deferred(result)

    def v2_runner_item_on_failed(self, result):
        result_dict = dict(result._result)
        self._process_result_for_localhost(result, is_task=False)

        if result_dict.get('msg', '').startswith('MODULE FAILURE'):
            self._log_module_failure(result, result_dict)
        elif result._task.action not in ('command', 'shell'):
            self._log_message(
                result=result,
                msg="Item: {item}".format(item=result_dict['item']),
                status='ERROR',
                result_dict=result_dict)
        else:
            stdout_lines = logconfig.filter_result(result_dict)
            for line in stdout_lines:
                hostname = self._get_hostname(result)
                self._log("%s | %s " % (hostname, line))

            self._log_message(
                result, "Item: {item} Result: {rc}".format(**result_dict))

        if self._deferred_result:
            self._process_deferred(result)

    def v2_playbook_on_stats(self, stats):
        # Add a spacer line before the stats so that there will be a line
        # between the last task and the recap
        self._log("")

        self._log("PLAY RECAP")

        hosts = sorted(stats.processed.keys())
        for host in hosts:
            t = stats.summarize(host)
            msg = (
                "{host} |"
                " ok: {ok}"
                " changed: {changed}"
                " unreachable: {unreachable}"
                " failed: {failures}".format(host=host, **t))

            # NOTE(pabelanger) Ansible 2.8 added rescued support
            if 'rescued' in t:
                # Even though skipped was in stable-2.7 and lower, only
                # stable-2.8 started rendering it. So just lump into rescued
                # check.
                msg += " skipped: {skipped} rescued: {rescued}".format(**t)
            # NOTE(pabelanger) Ansible 2.8 added ignored support
            if 'ignored' in t:
                msg += " ignored: {ignored}".format(**t)
            self._log(msg)

        # Add a spacer line after the stats so that there will be a line
        # between each playbook
        self._log("")

    def _process_deferred(self, result):
        self._items_done = True
        result_dict = self._deferred_result
        self._deferred_result = None
        status = result_dict.get('status')

        if status:
            self._log_message(result, "All items complete", status=status)

        # Log an extra blank line to get space after each task
        self._log("")

    def _print_task_banner(self, task):

        task_name = task.get_name().strip()

        if task.loop:
            task_type = 'LOOP'
        else:
            task_type = 'TASK'

        # TODO(mordred) With the removal of printing task args, do we really
        # want to keep doing this section?
        task_args = task.args.copy()
        is_shell = task_args.pop('_uses_shell', False)
        if is_shell and task_name == 'command':
            task_name = 'shell'
        raw_params = task_args.pop('_raw_params', '').split('\n')
        # If there's just a single line, go ahead and print it
        if len(raw_params) == 1 and task_name in ('shell', 'command'):
            task_name = '{name}: {command}'.format(
                name=task_name, command=raw_params[0])

        msg = "{task_type} [{task}]".format(
            task_type=task_type,
            task=task_name)
        self._log(msg)
        return task

    def _get_task_hosts(self, task):
        result = []

        # _restriction returns the parsed/compiled list of hosts after
        # applying subsets/limits
        hosts = self._play._variable_manager._inventory._restriction
        for inventory_host in hosts:
            # If this task has as delegate to, we don't care about the play
            # hosts, we care about the task's delegate target.
            if task.delegate_to:
                host = task.delegate_to
            else:
                host = inventory_host
            result.append((host, inventory_host))

        return result

    def _dump_result_dict(self, result_dict):
        result_dict = result_dict.copy()
        for key in list(result_dict.keys()):
            if key.startswith('_ansible'):
                del result_dict[key]
        logconfig.filter_result(result_dict)
        return result_dict

    def _log_message(self, result, msg=None, status="ok", result_dict=None):
        hostname = self._get_hostname(result)
        if result_dict:
            result_dict = self._dump_result_dict(result_dict)
        if result._task.no_log:
            self._log("{host} | {msg}".format(
                host=hostname,
                msg="Output suppressed because no_log was given"))
            return
        if (not msg and result_dict
                and set(result_dict.keys()) == set(['msg', 'failed'])):
            msg = result_dict['msg']
            result_dict = None
        if msg:
            msg_lines = msg.rstrip().split('\n')
            if len(msg_lines) > 1:
                self._log("{host} | {status}:".format(
                    host=hostname, status=status))
                for msg_line in msg_lines:
                    self._log("{host} | {msg_line}".format(
                        host=hostname, msg_line=msg_line))
            else:
                self._log("{host} | {status}: {msg}".format(
                    host=hostname, status=status, msg=msg))
        else:
            self._log("{host} | {status}".format(
                host=hostname, status=status, msg=msg))
        if result_dict:
            result_string = json.dumps(result_dict, indent=2, sort_keys=True)
            for line in result_string.split('\n'):
                self._log("{host} | {line}".format(host=hostname, line=line))

    def _log_module_failure(self, result, result_dict):
        if 'module_stdout' in result_dict and result_dict['module_stdout']:
            self._log_message(
                result, status='MODULE FAILURE',
                msg=result_dict['module_stdout'])
        elif 'exception' in result_dict and result_dict['exception']:
            self._log_message(
                result, status='MODULE FAILURE',
                msg=result_dict['exception'])
        elif 'module_stderr' in result_dict:
            self._log_message(
                result, status='MODULE FAILURE',
                msg=result_dict['module_stderr'])


    def _get_hostname(self, result):
        delegated_vars = result._result.get('_ansible_delegated_vars', None)
        if delegated_vars:
            return "{host} -> {delegated_host}".format(
                host=result._host.get_name(),
                delegated_host=delegated_vars['ansible_host'])
        else:
            return result._host.get_name()

    v2_runner_on_unreachable = v2_runner_on_failed
