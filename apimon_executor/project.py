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
import logging.config
import subprocess

from pathlib import Path
from git import Repo


class Project(object):

    log = logging.getLogger('apimon_executor.project')

    def __init__(self, name, repo_url, repo_ref, project_type, location,
                 exec_cmd, work_dir, **kwargs):
        self.name = name
        self.repo_url = repo_url
        self.repo_ref = repo_ref
        self.project_type = project_type
        self.tests_location = location
        self.exec_cmd = exec_cmd
        self.work_dir = work_dir
        self.project_dir = Path(self.work_dir, name)
        self.repo = None
        for (k, v) in kwargs.items():
            setattr(self, k, v)

    def prepare(self):
        self.repo = self.get_git_repo()
        # Try to install requirements
        requirements_file = Path(self.project_dir, 'requirements.yml')
        if (self.project_type.lower() == 'ansible' and
                requirements_file.exists()):
            process = subprocess.Popen(
                'ansible-galaxy install -r '
                '{file}'.format(file=requirements_file.as_posix()).split(' '),
                cwd=Path(self.work_dir).resolve(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            # Read the output
            for line in process.stdout:
                self.log.debug('%s', line.decode('utf-8'))
            stderr = process.stderr.read()
            if stderr:
                self.log.error('%s', stderr.decode('utf-8'))

    def get_git_repo(self):
        """Get a repository object
        """
        self.log.debug('Getting git repository: %s' % self.repo_url)
        git_path = Path(self.project_dir, '.git')
        if git_path.exists():
            self.repo = Repo(self.project_dir)
            self.refresh_git_repo()
        else:
            self.repo = Repo.clone_from(self.repo_url, self.project_dir,
                                        recurse_submodules='.')
            self.repo.remotes.origin.pull(self.repo_ref)
        return self.repo

    def refresh_git_repo(self):
        try:
            if not self.repo:
                self.repo = self.get_git_repo()
            self.repo.remotes.origin.pull(self.repo_ref,
                                          recurse_submodules=True)
        except Exception:
            self.log.exception('Cannot update repository')

    def is_repo_update_necessary(self):
        """Check whether update of the repository is necessary

        Returns True, when a commit checksum remotely differs from local state
        """
        self.log.debug('Checking whether there are remote changes in git')
        try:
            self.repo.remotes.origin.update()
            origin_ref = self.repo.remotes.origin.refs[self.repo_ref]
            last_commit_remote = origin_ref.commit
            last_commit_local = self.repo.refs[self.repo_ref].commit
            return last_commit_remote != last_commit_local
        except Exception:
            self.log.exception('Cannot update git remote')
            return False

    def tasks(self):
        self.log.debug('Looking for tasks')
        if hasattr(self, 'scenarios'):
            for scenario in self.scenarios:
                scenario_file = Path(self.project_dir,
                                     self.tests_location, scenario)
                if scenario_file.exists():
                    yield scenario_file.relative_to(
                        self.project_dir).as_posix()
                else:
                    self.log.warning('Requested scenario %s does not exist',
                                     scenario)
        else:
            for file in Path(
                    self.project_dir,
                    self.tests_location).glob('scenario*.yaml'):
                self.log.debug('Scheduling %s', file)
                yield file.relative_to(self.project_dir).as_posix()

    def get_exec_cmd(self, task):
        return self.exec_cmd % (task)

    def is_task_valid(self, task):
        task_path = Path(self.project_dir, task)
        return task_path.exists()
