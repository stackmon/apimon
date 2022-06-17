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
import subprocess
from pathlib import Path

from git import Repo


class Project(object):

    log = logging.getLogger('apimon.Project')

    def __init__(self, name, repo_url, repo_ref='master',
                 type='ansible', location='playbooks',
                 exec_cmd='ansible-playbook -i inventory/testing %s -v',
                 work_dir='wrk', **kwargs):
        self.name = name
        self.repo_url = repo_url
        self.repo_ref = repo_ref
        self.type = type
        self.location = location
        self.exec_cmd = exec_cmd
        self.work_dir = work_dir
        self.project_dir = Path(self.work_dir, name)
        self.repo = None
        self._tasks = []
        self.scenarios = []
        for (k, v) in kwargs.items():
            setattr(self, k, v)

    def _set_work_dir(self, work_dir):
        self.work_dir = work_dir
        self.project_dir = Path(self.work_dir, self.name)

    def _ansible_galaxy_install(self, object_type='role',
                                requirements_file='requirements.yml'):
        proc = subprocess.Popen(
            'ansible-galaxy {type} install -r '
            '{file}'.format(
                type=object_type,
                file=requirements_file.resolve()).split(' '),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        self.log.debug('Installing galaxy deps')

        for line in proc.stdout:
            self.log.info('%s', line.decode('utf-8'))
        stderr = proc.stderr.read()
        if stderr:
            self.log.error('%s', stderr.decode('utf-8'))

        proc.wait()

        return proc.returncode

    def prepare(self, work_dir=None) -> int:
        """Do some preparation steps before executing the task"""
        if work_dir:
            self._set_work_dir(work_dir)
        if not self.work_dir:
            raise RuntimeError('Work dir is not set')
        self.repo = self.get_git_repo()
        # Try to install requirements
        requirements_file = Path(self.project_dir, 'requirements.yml')
        if (self.type.lower() == 'ansible' and
                requirements_file.exists()):
            rc = self._ansible_galaxy_install('role', requirements_file)
            if rc == 0:
                rc = self._ansible_galaxy_install(
                    'collection', requirements_file)
            if rc != 0:
                self.log.error('Error installing dependencies: RC=%s', rc)
            return rc
        return 0

    def get_git_repo(self) -> Repo:
        """Get a repository object
        """
        if not self.project_dir:
            raise RuntimeError('Project work dir is not set')
        self.log.debug('Getting git repository: %s', self.repo_url)
        if self.repo:
            return self.repo
        git_path = Path(self.project_dir, '.git')
        if git_path.exists():
            self.repo = Repo(self.project_dir)
        else:
            self.log.debug('Checking out repository')
            self.repo = Repo.clone_from(self.repo_url, self.project_dir,
                                        recurse_submodules='.')
        self.refresh_git_repo()
        return self.repo

    def refresh_git_repo(self, recurse_submodules=True):
        """Refresh git repository"""
        try:
            if not self.repo:
                self.repo = self.get_git_repo()
            # Fetch origin
            self.repo.remotes.origin.fetch()
            # Checkout desired branch
            self.repo.refs[self.repo_ref].checkout()
        except Exception:
            self.log.exception('Cannot update repository')

    def is_repo_update_necessary(self):
        """Check whether update of the repository is necessary

        Returns True, when a commit checksum remotely differs from local state
        """
        self.log.debug('Checking whether there are remote changes in git')
        try:
            if not self.repo:
                self.get_git_repo()
            # Fetch origin
            self.repo.remotes.origin.fetch()
            # Get origin target ref to see last commit
            origin_ref = self.repo.remotes.origin.refs[self.repo_ref]
            last_commit_remote = origin_ref.commit
            last_commit_local = self.repo.head.commit
            remote_newer = last_commit_remote != last_commit_local
            if remote_newer:
                self.log.info('Found new revision in git. Update necessary')
            return remote_newer
        except Exception:
            self.log.exception('Cannot update git remote')
#            self.repo = None
            return False

    def get_commit(self):
        """Returns commit hash of the HEAD"""
        return self.repo.head.commit

    def tasks(self):
        """Return generator of tasks in the project"""
        if not self._tasks:
            self._find_tasks()
        for task in self._tasks:
            yield task

    def _find_tasks(self):
        """Identify tasks that can be scheduled"""
        self._tasks = []
        if False and hasattr(self, 'scenarios') and self.scenarios:
            # NOTE(gtema): disabled for now
            for scenario in self.scenarios:
                scenario_file = Path(self.project_dir,
                                     self.location, scenario)
                if scenario_file.exists():
                    self._tasks.append(scenario_file.relative_to(
                        self.project_dir).as_posix())
                else:
                    self.log.warning('Requested scenario %s does not exist',
                                     scenario)
        else:
            for scenario in Path(
                    self.project_dir,
                    self.location).glob('scenario*.yaml'):
                if not scenario.exists():
                    continue
                self._tasks.append(
                    scenario.relative_to(self.project_dir).as_posix())

    def get_exec_cmd(self, task):
        """Render the execution command for the task"""
        return self.exec_cmd % (task)

    def is_task_valid(self, task):
        """Evaluate whether we can run the task"""
        task_path = Path(self.project_dir, task)
        exists = task_path.exists()
        if not exists:
            return False
        elif hasattr(self, 'scenarios') and self.scenarios:
            if task in [Path(self.location, value).as_posix() for value
                        in self.scenarios]:
                return True
            else:
                return False
        else:
            return True
