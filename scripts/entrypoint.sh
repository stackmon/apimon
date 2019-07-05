#!/bin/bash

mkdir -p /usr/app
cd /usr/app

chmod u+x executor.py

if [[ ! ""${EXECUTOR_REPO_URL}" ]]; then
    echo -e "\$EXECUTOR_REPO_URL variable must be set.\n"
    exit 1
else
    repo=${EXECUTOR_REPO_URL}
fi

git clone $repo api-monitoring

cd api-monitoring

git pull

ansible-galaxy install -r requirements.yml

workdir=/usr/app/api_monitoring
git_ref=${EXECUTOR_GIT_REF:-master}
scenarios_location=${EXECUTOR_SCENARIOS_LOCATION:-playbooks/scenarios}
interval=${EXECUTOR_REFRESH_INTERVAL:-120}
count_executors=${EXECUTOR_COUNT_EXECUTORS:-10}

executor.py ${repo} --work_dir ${work_dir} --ref ${git_ref} --location ${SCENARIOS_LOCATION} --interval ${interval} --count_executor ${count_executors}

wait
