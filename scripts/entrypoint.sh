#!/bin/bash

mkdir -p /usr/app
cd /usr/app

git_dir=/usr/app/test_repo

chmod u+x executor.py

if [[ ! "${EXECUTOR_REPO_URL}" ]]; then
    echo -e "\$EXECUTOR_REPO_URL variable must be set.\n"
    exit 1
else
    repo=${EXECUTOR_REPO_URL}
fi

git clone ${repo} ${git_dir} --recurse-submodules

if [ -f "${git_dir}/requirements.yml" ]; then
    ansible-galaxy install -r ${git_dir}/requirements.yml
fi

config=${EXECUTOR_CONFIG:-/etc/apimon_executor/executor.yaml}
git_ref=${EXECUTOR_GIT_REF:-master}
scenarios_location=${EXECUTOR_SCENARIOS_LOCATION:-playbooks/scenarios}
interval=${EXECUTOR_REFRESH_INTERVAL:-120}
count_executors=${EXECUTOR_COUNT_EXECUTORS:-10}

apimon-scheduler --config ${config} --repo ${repo} \
    --git-checkout-dir ${work_dir} \
    --git-ref ${git_ref} --location ${scenarios_location} \
    --git-refresh-interval ${interval} \
    --countr-executor-threads ${count_executors}

wait
