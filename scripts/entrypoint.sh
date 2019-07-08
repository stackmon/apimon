#!/bin/bash

mkdir -p /usr/app
cd /usr/app

work_dir=/usr/app/test_repo

chmod u+x executor.py

if [[ ! "${EXECUTOR_REPO_URL}" ]]; then
    echo -e "\$EXECUTOR_REPO_URL variable must be set.\n"
    exit 1
else
    repo=${EXECUTOR_REPO_URL}
fi

git clone ${repo} ${work_dir}

if [ -f "${work_dir}/requirements.yml" ]; then
    ansible-galaxy install -r ${work_dir}/requirements.yml
fi

git_ref=${EXECUTOR_GIT_REF:-master}
scenarios_location=${EXECUTOR_SCENARIOS_LOCATION:-playbooks/scenarios}
interval=${EXECUTOR_REFRESH_INTERVAL:-120}
count_executors=${EXECUTOR_COUNT_EXECUTORS:-10}

./executor.py ${repo} --work_dir ${work_dir} --ref ${git_ref} --location ${scenarios_location} --interval ${interval} --count_executor ${count_executors}

wait
