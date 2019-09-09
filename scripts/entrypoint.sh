#!/bin/bash

mkdir -p /usr/app
cd /usr/app

git_dir=/usr/app/test_repo

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

apimon-scheduler --config ${config}
