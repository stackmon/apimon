#!/bin/bash

mkdir -p /usr/app
cd /usr/app

git_dir=/usr/app/test_repo

config=${EXECUTOR_CONFIG:-/etc/apimon_executor/executor.yaml}

apimon-scheduler --config ${config}

wait
