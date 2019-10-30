#!/bin/bash

mkdir -p /usr/app
cd /usr/app

config=${EXECUTOR_CONFIG:-/etc/apimon_executor/executor.yaml}

apimon-scheduler --config ${config}
