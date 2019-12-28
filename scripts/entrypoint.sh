#!/bin/bash

prep_term()
{
    unset term_child_pid
    unset term_kill_needed
    trap 'handle_term' TERM INT
}

handle_term()
{
    if [ "${term_child_pid}" ]; then
        kill -TERM "${term_child_pid}" 2>/dev/null
    else
        term_kill_needed="yes"
    fi
}

wait_term()
{
    term_child_pid=$!
    if [ "${term_kill_needed}" ]; then
        kill -TERM "${term_child_pid}" 2>/dev/null
    fi
    wait ${term_child_pid}
    trap - TERM INT
    wait ${term_child_pid}
}

mkdir -p /usr/app
cd /usr/app

config=${EXECUTOR_CONFIG:-/etc/apimon_executor/executor.yaml}

prep_term
apimon-scheduler --config ${config} &
wait_term
