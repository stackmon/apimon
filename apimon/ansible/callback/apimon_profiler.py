# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

import collections
import os
import re
import socket
import time

from ansible.module_utils._text import to_text
from ansible.module_utils.six.moves import reduce
from ansible.plugins.callback import CallbackBase

from pathlib import PurePosixPath

from apimon.executor import message


DOCUMENTATION = '''
    callback: apimon_profiler
    type: aggregate
    short_description: adds time statistics about invoked OpenStack modules
    version_added: "2.9"
    description:
      - Ansible callback plugin for timing individual APImon related tasks and
        overall execution time.
    requirements:
      - whitelist in configuration - see examples section below for details.
      - influxdb python client for writing metrics to influxdb
    options:
      use_last_name_segment:
          description: Use only last part of the name after colon sign as name
          default: True
          type: boolean
          env:
              - name: APIMON_PROFILER_USE_LAST_NAME_SEGMENT
          ini:
      socket:
          description: APImon socket for metrics
          default: ".comm_socket"
          type: str
          env:
              - name: APIMON_PROFILER_MESSAGE_SOCKET
          ini:
              - section: callback_apimon_profiler
                key: socket

'''

EXAMPLES = '''
example: >
  To enable, add this to your ansible.cfg file in the defaults block
    [defaults]
    callback_whitelist = apimon_profiler
sample output: >
Monday 22 July 2019  18:06:55 +0200 (0:00:03.034)       0:00:03.034 ***********
===============================================================================
Action=os_auth, state=None duration=1.19, changed=False, name=Get Token
Action=script, state=None duration=1.48, changed=True, name=List Keypairs
Overall duration of APImon tasks in playbook playbooks/scenarios/sc1_tst.yaml
    is: 2675.616 ms
Playbook run took 0 days, 0 hours, 0 minutes, 2 seconds

'''


# define start time
t0 = tn = time.time_ns()
te = 0

rc_str_struct = {
    -1: 'Undefined',
    0: 'Passed',
    1: 'Skipped',
    2: 'FailedIgnored',
    3: 'Failed'
}


def secondsToStr(t):
    # http://bytes.com/topic/python/answers/635958-handy-short-cut-formatting-elapsed-time-floating-point-seconds
    def rediv(ll, b):
        return list(divmod(ll[0], b)) + ll[1:]

    return "%d:%02d:%02d.%03d" % tuple(
        reduce(rediv, [[t * 1000, ], 1000, 60, 60]))


def filled(msg, fchar="*"):
    if len(msg) == 0:
        width = 79
    else:
        msg = "%s " % msg
        width = 79 - len(msg)
    if width < 3:
        width = 3
    filler = fchar * width
    return "%s%s " % (msg, filler)


def timestamp(self):
    if self.current is not None:
        self.stats[self.current]['time'] = time.time() - \
            self.stats[self.current]['time']


def tasktime():
    global tn
    time_current = time.strftime('%A %d %B %Y  %H:%M:%S %z')
    time_elapsed = secondsToStr((te - tn) / 1000000000)
    time_total_elapsed = secondsToStr((te - t0) / 1000000000)
    tn = time.time_ns()
    return filled('%s (%s)%s%s' %
                  (time_current, time_elapsed, ' ' * 7, time_total_elapsed))


class CallbackModule(CallbackBase):
    """Callback ansible module

    This callback module processes information about each task and report
    individual statistics into influxdb.
    """
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'aggregate'
    CALLBACK_NAME = 'apimon_profiler'
    CALLBACK_NEEDS_WHITELIST = True

    ACTION_SERVICE_MAP = {
        'auth': 'identity',
        'availability_zone_info': 'block_storage',
        'client_config': 'general',
        'group': 'identity',
        'group_info': 'identity',
        'keystone_domain': 'identity',
        'keystone_domain_info': 'identity',
        'keystone_endpoint': 'identity',
        'keystone_role': 'identity',
        'keystone_service': 'identity',
        'project': 'identity',
        'project_access': 'identity',
        'project_info': 'identity',
        'user': 'identity',
        'user_group': 'identity',
        'user_info': 'identity',
        'user_role': 'identity',
        'keystone_domain_facts': 'identity',
        'project_facts': 'identity',
        'quota': 'quota',
        'object': 'object_store',
        'image': 'image',
        'image_info': 'image',
        'flavor': 'compute',
        'flavor_info': 'compute',
        'server': 'compute',
        'server_info': 'compute',
        'server_action': 'compute',
        'server_group': 'compute',
        'server_volume': 'compute',
        'server_metadata': 'compute',
        'keypair': 'compute',
        'keypair_info': 'compute',
        'floating_ip': 'network',
        'router': 'network',
        'router_info': 'network',
        'routers_info': 'network',
        'network': 'network',
        'network_info': 'network',
        'networks_info': 'network',
        'subnet': 'network',
        'subnet_info': 'network',
        'subnets_info': 'network',
        'port': 'network',
        'port_info': 'network',
        'security_group': 'network',
        'security_group_info': 'network',
        'security_group_rule': 'network',
        'security_group_rule_info': 'network',
        'listener': 'loadbalancer',
        'loadbalancer': 'loadbalancer',
        'member': 'loadbalancer',
        'pool': 'loadbalancer',
        'volume': 'block_storage',
        'volume_info': 'block_storage',
        'volume_snapshot': 'block_storage',
        'volume_snapshot_info': 'block_storage',
        'volume_backup': 'block_storage',
        'volume_backup_info': 'block_storage',
        'stack': 'orchestrate',
        'stack_info': 'orchestrate',
        'dns_zone': 'dns',
        'dns_zone_info': 'dns',
        'recordset': 'dns',
        'recordset_info': 'dns',
        'otc_listener': 'loadbalancer',
        'otc_loadbalancer': 'loadbalancer',
        'otc_member': 'loadbalancer',
        'otc_pool': 'loadbalancer',
        'rds_datastore_info': 'rds',
        'rds_flavor_info': 'rds',
        'as_config_info': 'as',
        'as_group_info': 'as',
        'cce_cluster': 'cce',
        'cce_cluster_info': 'cce',
        'cce_cluster_node': 'cce',
        'cce_cluster_node_info': 'cce',
        'cce_cluster_cert_info': 'cce',
    }

    def __init__(self):
        self.stats = collections.OrderedDict()
        self.current = None
        self.playbook_name = None
        self._metrics = collections.OrderedDict()

        super(CallbackModule, self).__init__()

    def set_options(self, task_keys=None, var_options=None, direct=None):

        super(CallbackModule, self).set_options(task_keys=task_keys,
                                                var_options=var_options,
                                                direct=direct)

        self.use_last_name_segment = self.get_option('use_last_name_segment')

        self.job_id = os.getenv('TASK_EXECUTOR_JOB_ID')
        self.environment = os.getenv('APIMON_PROFILER_ENVIRONMENT',
                                     'Production')
        self.customer = os.getenv('APIMON_PROFILER_ALERTA_CUSTOMER')
        self.origin = os.getenv('APIMON_PROFILER_ORIGIN')

        self.message_socket_address = self.get_option('socket')

    def v2_playbook_on_start(self, playbook):
        if not self.playbook_name:
            self.playbook_name = playbook._file_name

    def is_task_interesting(self, task):
        return (
            isinstance(task.action, str) and
            (
                task.action.startswith('os_')
                or task.action.startswith('otc')
                or task.action.startswith('opentelekomcloud')
                or task.action.startswith('openstack')
                # This is bad, but what else can we do?
                or task.action[:3] in ['rds', 'cce']
                or task.action in ('script', 'command',
                                   'wait_for_connection', 'wait_for')
            )
        )

    def v2_playbook_on_task_start(self, task, is_conditional):
        # NOTE(gtema): used attrs might be jinjas, so probably
        # need to update value with the invokation values
        play = task._parent._play
        self._display.vvv('Profiler: task start %s' % (task.dump_attrs()))
        if self.is_task_interesting(task):
            self.current = task._uuid
            if task.action == 'script' and task.get_name() == 'script':
                name = task.args.get('_raw_params')
            else:
                name = task.get_name()
            if self.use_last_name_segment and ':' in name:
                # When we invoke role - it's name is part of the name.
                # Just take the last segment after ':'
                name_parts = name.split(':')
                name = name_parts[-1].strip()
            action = None
            if (
                task.action.startswith('opentelekomcloud.')
                or task.action.startswith('openstack.')
            ):
                name_parts = task.action.split('.')
                if len(name_parts) > 1:
                    # action is last segment after '.'
                    action = name_parts[-1]
                else:
                    action = task.action
            else:
                action = task.action

            stat_args = {
                'start': time.time_ns(),
                'name': name,
                'long_name': '{play}:{name}'.format(
                    play=play, name=name),
                'action': action,
                'task': task,
                'play': task._parent._play.get_name(),
            }
            state = to_text(task.args.get('state'))
            if state != '{{ state }}':
                stat_args['state'] = state
            az = task.args.get('availability_zone')
            service = None
            metrics = []

            for tag in task.tags:
                # Look for tags on interesting tags
                if 'az=' == tag[:3] and not az:
                    az = tag[3:]
                if tag.startswith('service='):
                    service = tag[8:]
                if tag.startswith('metric='):
                    metrics.append(to_text(tag[7:]))

            if not service and action in self.ACTION_SERVICE_MAP:
                service = self.ACTION_SERVICE_MAP[action]
            if not service:
                # A very nasty fallback
                if action.startswith('wait_for'):
                    service = 'compute'
                # We do not know the action>service mapping. Try to get first
                # part of the name before '_'
                name_parts = action.split('_')
                if len(name_parts) > 1:
                    service = name_parts[0]
                else:
                    service = action

            if az:
                stat_args['az'] = to_text(az)
            if service:
                stat_args['service'] = to_text(service)
            if metrics:
                stat_args['metrics'] = metrics

            self.stats[self.current] = stat_args
            if self._display.verbosity >= 2:
                self.stats[self.current]['path'] = task.get_path()
        else:
            self.current = None

    def _update_task_stats(self, result, rc):
        if self.current is not None:
            duration = time.time_ns() - self.stats[self.current]['start']
            # NS to MS
            duration = int(duration / 1000000)

            invoked_args = result._result.get('invocation')

            attrs = {
                'changed': result._result.get('changed', 'False'),
                'end': time.time_ns(),
                'duration': duration,
                'rc': rc
            }
            if (isinstance(invoked_args, dict)
                    and 'module_args' in invoked_args):
                module_args = invoked_args.get('module_args')
                if (
                    'availability_zone' in module_args
                    and module_args['availability_zone']
                ):
                    attrs['az'] = module_args['availability_zone']
                    if not attrs['az']:
                        attrs['az'] = 'default'
                if 'state' in module_args:
                    attrs['state'] = module_args.get('state')
                if rc == 3:
                    msg = None
                    if 'msg' in result._result:
                        msg = result._result['msg']
                    attrs['raw_response'] = msg
                    try:
                        attrs['anonymized_response'] = \
                            self._anonymize_message(attrs['raw_response'])
                        attrs['error_category'] = \
                            self._get_message_error_category(
                            attrs['anonymized_response'])
                    except Exception:
                        pass
            else:
                if rc == 3:
                    msg = None
                    if 'stderr_lines' in result._result:
                        msg = result._result['stderr_lines'][-1]
                    elif 'module_stderr' in result._result:
                        msg = result._result['module_stderr'].splitlines()[-1]
                    attrs['raw_response'] = msg
                    try:
                        attrs['anonymized_response'] = \
                            self._anonymize_message(attrs['raw_response'])
                        attrs['error_category'] = \
                            self._get_message_error_category(
                            attrs['anonymized_response'])
                    except Exception:
                        pass

            self.stats[self.current].update(attrs)

            try:
                if 'metrics' in self.stats[self.current]:
                    az = attrs.get('az')
                    if not az and 'az' in self.stats[self.current]:
                        az = self.stats[self.current]['az']
                    for metric_name in self.stats[self.current]['metrics']:

                        if metric_name not in self._metrics:
                            self._metrics[metric_name] = {}
                        metric_obj = self._metrics[metric_name]
                        metric = None
                        if 'az':
                            metric = metric_obj.get(az, {})
                        if not metric:
                            metric = {}

                        metric_attrs = {
                            'duration': metric.get('duration', 0) + duration,
                            'rc': rc
                        }
                        if az:
                            metric_obj[az] = metric_attrs
                        else:
                            metric_obj = metric_attrs
                        self._metrics[metric_name] = metric_obj
            except Exception as e:
                self._display.error('Error trying to update metrics: %s' % e)

            task_data = self.stats[self.current]
            prof_task = message.ResultTask(
                name=task_data.get('name'), result=rc,
                duration=duration,
                action=task_data.get('action'),
                play=task_data.get('play'),
                long_name=task_data.get('long_name'),
                state=task_data.get('state'),
                az=task_data.get('az'),
                raw_response=task_data.get('raw_response'),
                anonymized_response=task_data.get('anonymized_response'),
                service=task_data.get('service')
            )

            self._emit_message(prof_task)

    def v2_runner_on_skipped(self, result):
        # Task was skipped - remove stats
        self._update_task_stats(result, 1)

    def v2_runner_on_ok(self, result):
        self._display.vvvv('Profiler: result: %s' % result._result)
        self._update_task_stats(result, 0)

    def v2_runner_on_failed(self, result, ignore_errors=False):
        self._display.vvvv('Profiler: result: %s' % result._result)
        rc = 3 if not ignore_errors else 2
        self._update_task_stats(result, rc)

    def _emit_message(self, data: dict) -> None:
        if self.message_socket_address:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as _socket:
                try:
                    _socket.connect(self.message_socket_address)
                    msg = '%s\n' % data.serialize()
                    _socket.sendall(msg.encode('utf8'))
                except socket.error as ex:
                    self._display.v(
                        'Error establising connection to socket %s' %
                        ex)
                except Exception as e:
                    self._display.v('Error writing message to socket %s' % e)

    def v2_playbook_on_stats(self, stats):
        global te, t0

        te = time.time_ns()
        self._display.display(tasktime())
        self._display.display(filled("", fchar="="))

        results = self.stats.items()

        self._display.vvv('Metrics to be emitted are: %s' % self._metrics)
        overall_apimon_duration = 0
        rcs = {
            0: 0,
            1: 0,
            2: 0,
            3: 0
        }

        # Print the timings
        for uuid, result in results:
            duration = result['duration']
            overall_apimon_duration = overall_apimon_duration + duration
            rcs.update({result['rc']: rcs[result['rc']] + 1})
            msg = u"Action={0}, state={1} duration={2:.02f}, " \
                  u"changed={3}, name={4}".format(
                      result.get('action'),
                      result.get('state'),
                      duration,
                      result.get('changed'),
                      result.get('name')
                  )
            self._display.display(msg)

        playbook_name = PurePosixPath(self.playbook_name).name

        if True or self.influxdb_client:
            try:
                rescued = 0
                for (host, val) in stats.rescued.items():
                    if val:
                        rescued += val
                playbook_rc = 0 if (rcs[3] == 0 and rescued == 0) else 3
                prof_summary = message.ResultSummary(
                    name=playbook_name, result=playbook_rc,
                    duration=int((te - t0) / 1000000),
                    count_passed=rcs[0],
                    count_skipped=rcs[1],
                    count_ignored=rcs[2],
                    count_failed=rcs[3]
                )
                self._emit_message(prof_summary)
            except Exception as e:
                self._display.error('Error sending metrics: %s' % e)

            try:
                if self._metrics:
                    data = []
                    for name, metric in self._metrics.items():
                        if 'rc' not in metric:
                            for az, vals in metric.items():
                                data.extend(self._get_metric_data(
                                    name, vals,
                                    playbook_name=playbook_name, az=az))
                        else:
                            data.extend(self._get_metric_data(
                                name, metric,
                                playbook_name=playbook_name))
                    if data:
                        for entry in data:
                            self._emit_message(entry)

            except Exception as e:
                self._display.warning('Error emitting additional metrics: %s' %
                                      e)

        self._display.display(
            'Overall duration of APImon tasks in playbook %s is: %s s' %
            (self.playbook_name, str(overall_apimon_duration / 1000)))

    def _get_metric_data(self, name: str,
                         vals: dict, **kwargs) -> dict:
        rc = int(vals.get('rc', -1))
        dt = []
#            message.Metric(
#                '%s.attempted' % (name),
#                value=1,
#                metric_type='c',
#                **kwargs
#            ),
#            message.Metric(
#                '%s.rc_%s' % (name, str(rc)),
#                value=1,
#                metric_type='c',
#                **kwargs
#            )
#        ]
        if rc == 0:
            kwargs['name_suffix'] = 'passed'
        elif rc == 1:
            kwargs['name_suffix'] = 'skipped'
        elif rc == 2:
            kwargs['name_suffix'] = 'failedignored'
        elif rc == 3:
            kwargs['name_suffix'] = 'failed'
        dt.append(message.Metric(
            name,
            int(vals.get('duration', -1)),
            metric_type='ms',
            **kwargs
        ))
        return dt

    def _anonymize_message(self, msg: str) -> str:
        # Anonymize remaining part
        # Project_id
        result = msg
        result = re.sub(r"[0-9a-z]{32}", "_omit_", result)
        # UUID
        result = re.sub(r"([0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}"
                        "-[0-9a-z]{4}-[0-9a-z]{12})", "_omit_", result)
        # WAF_ID likes
        result = re.sub(r"[0-9]{5,}", "_omit_", result)
        # Scenario random
        result = re.sub(r"-[0-9a-zA-Z]{12}-", "_omit_", result)
        # tmp12345678
        result = re.sub(r"tmp[0-9a-zA-Z]{8}", "_omit_", result)
        # IP
        result = re.sub(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d*)?",
                        "_omit_", result)

        return result
