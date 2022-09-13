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
#
import tempfile
from pathlib import Path
from unittest import TestCase

from apimon import project as _project

from git import Repo
from git import SymbolicReference

import mock


class TestProject(TestCase):

    def setUp(self):
        super(TestProject, self).setUp()
        self.project = _project.Project('fake_proj', 'fake_url', 'master',
                                        'ansible', 'fake_loc', 'fake_cmd %s',
                                        'wrk_dir')

    def test_basic(self):
        self.assertEqual('fake_proj', self.project.name)
        self.assertEqual('fake_url', self.project.repo_url)
        self.assertEqual('master', self.project.repo_ref)
        self.assertEqual('ansible', self.project.type)
        self.assertEqual('fake_loc', self.project.location)
        self.assertEqual('fake_cmd %s', self.project.exec_cmd)
        self.assertEqual('wrk_dir', self.project.work_dir)

    def test_get_git_repo_present(self):
        with tempfile.TemporaryDirectory() as tmp_dir:

            repo_dir = Path(tmp_dir, 'fake_proj')

            Repo.init(repo_dir)
            prj = _project.Project('fake_proj', 'fake_url', 'master',
                                   'ansible', 'fake_loc', 'fake_cmd %%s',
                                   tmp_dir)

            with mock.patch.object(prj, 'repo') as git_mock:
                prj.get_git_repo()
                git_mock.git.checkout.assert_called()

    @mock.patch.object(Repo, 'clone_from')
    def test_get_git_repo_fresh(self, git_mock):
        repo = mock.Mock(autospec=Repo)
        git_mock.return_value = repo

        self.project.get_git_repo()

        git_mock.assert_called_with('fake_url',
                                    Path('wrk_dir', 'fake_proj'),
                                    recurse_submodules='.',
                                    branch='master')

    def test_refresh_git_repo(self):
        with mock.patch.object(self.project, 'repo') as repo_mock:
            self.assertIsNone(self.project.refresh_git_repo())
            repo_mock.remotes.origin.update.assert_called()

    def test_refresh_git_repo_branch(self):
        self.project.repo_ref = 'branch'
        with mock.patch.object(self.project, 'repo') as repo_mock:
            self.assertIsNone(self.project.refresh_git_repo())
            repo_mock.remotes.origin.update.assert_called()

    def test_is_repo_update_necessary(self):
        with mock.patch.object(self.project, 'repo') as repo_mock:
            remote_ref = mock.Mock()
            remote_ref.commit = 2
            local_ref = mock.Mock()
            local_ref.commit = 1

            repo_mock.remotes.origin.refs = {'master': remote_ref}
            repo_mock.head = local_ref
            self.assertTrue(self.project.is_repo_update_necessary())
            repo_mock.remotes.origin.update.assert_called()
            repo_mock.head = remote_ref
            self.assertFalse(self.project.is_repo_update_necessary())
            repo_mock.remotes.origin.update.assert_called()

    def test_git_real_repo(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_url = Path(tmp_dir, 'bare-repo')
            Repo.init(repo_url, bare=True)

            checkout = tempfile.TemporaryDirectory()
            control_repo = Repo.clone_from(repo_url, checkout.name)
            new_file_path = Path(control_repo.working_tree_dir, 'dummy')
            open(new_file_path, 'wb').close()
            control_repo.index.add([new_file_path.name])
            control_repo.index.commit("Initial commit")
            control_repo.remotes.origin.push()

            branch_name = 'another_branch'
            SymbolicReference.create(
                control_repo, "refs/remotes/origin/%s" % branch_name)

            control_repo.create_head(branch_name, 'main')
            control_repo.remotes[0].refs[branch_name]
            control_repo.git.push('--set-upstream', 'origin', branch_name)

            project_clone = tempfile.TemporaryDirectory()
            prj = _project.Project(
                'fake_project', repo_url, repo_ref=branch_name,
                work_dir=project_clone.name)

            self.assertFalse(prj.is_repo_update_necessary())

            control_repo.heads.another_branch.checkout()
            open(new_file_path, 'wb').close()
            control_repo.index.add([new_file_path.name])
            control_repo.index.commit("Update")
            control_repo.remotes.origin.push()

            self.assertTrue(prj.is_repo_update_necessary())
            prj.refresh_git_repo()

    def test_ansible_galaxy_install(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_dir = Path(tmp_dir, 'fake_proj')

            Repo.init(repo_dir)

            prj = _project.Project('fake_proj', 'fake_url', 'master',
                                   'ansible', 'fake_loc', 'fake_cmd %%s',
                                   tmp_dir)
            requirements_file = Path(repo_dir, 'requirements.yml')

            with mock.patch.object(prj, 'refresh_git_repo'), \
                    mock.patch('subprocess.Popen') as process_mock:
                open(requirements_file, 'a').close()
                prj._ansible_galaxy_install('role', requirements_file)
                process_mock.assert_called_with(
                    'ansible-galaxy role install -r {file}'.format(
                        file=Path(requirements_file).resolve()).split(' '),
                    stdout=-1, stderr=-1
                )
                prj._ansible_galaxy_install('collection', requirements_file)
                process_mock.assert_called_with(
                    'ansible-galaxy collection install -r {file}'.format(
                        file=Path(requirements_file).resolve()).split(' '),
                    stdout=-1, stderr=-1
                )

    def test_prepare_ansible(self):
        with tempfile.TemporaryDirectory() as tmp_dir:

            repo_dir = Path(tmp_dir, 'fake_proj')

            Repo.init(repo_dir)

            prj = _project.Project('fake_proj', 'fake_url', 'master',
                                   'ansible', 'fake_loc', 'fake_cmd %%s',
                                   tmp_dir)
            requirements_file = Path(repo_dir, 'requirements.yml')

            with mock.patch.object(prj, 'repo'), \
                    mock.patch.object(prj, '_ansible_galaxy_install') \
                    as install_mock:
                prj.prepare()
                install_mock.assert_not_called()
                install_mock.return_value = 0

                # Now write file and ensure galaxy invoked
                open(requirements_file, 'a').close()
                res = prj.prepare()
                calls = [
                    mock.call('role', requirements_file),
                    mock.call('collection', requirements_file)
                ]
                install_mock.assert_has_calls(calls)
                self.assertEqual(res, 0)

                install_mock.return_value = 1

                # Now write file and ensure galaxy invoked
                open(requirements_file, 'a').close()
                res = prj.prepare()
                calls = [
                    mock.call('role', requirements_file),
                ]
                install_mock.assert_has_calls(calls)
                self.assertEqual(res, 1)

    def test_tasks_all(self):
        with tempfile.TemporaryDirectory() as tmp_dir:

            repo_dir = Path(tmp_dir, 'fake_proj')
            repo_dir.mkdir()

            prj = _project.Project('fake_proj', 'fake_url', 'master',
                                   'ansible', 'fake_loc', 'fake_cmd %%s',
                                   tmp_dir)

            task_loc = Path(repo_dir, 'fake_loc')

            task_loc.mkdir()

            fake_tasks = ('scenario_task1.yaml', 'scenario_task2.yaml',
                          'scenario_task3.yaml')

            for task in fake_tasks:
                open(Path(task_loc, task).as_posix(), 'a').close()

            found_tasks = set(prj.tasks())
            self.assertEqual(found_tasks, set('fake_loc/' + v for v in
                                              fake_tasks))

#    def test_tasks_filtered(self):
#        with tempfile.TemporaryDirectory() as tmp_dir:
#
#            repo_dir = Path(tmp_dir, 'fake_proj')
#            repo_dir.mkdir()
#
#            scenarios = ('scenario_test1.tst', 'scenario_test2.tst')
#
#            prj = _project.Project('fake_proj', 'fake_url', 'master',
#                                   'ansible', 'fake_loc', 'fake_cmd %%s',
#                                   tmp_dir, scenarios=scenarios)
#
#            task_loc = Path(repo_dir, 'fake_loc')
#
#            task_loc.mkdir()
#
#            fake_tasks = ('scenario_test1.tst', 'scenario_test2.tst',
#                          'scenario3.yaml')
#
#            for task in fake_tasks:
#                open(Path(task_loc, task).as_posix(), 'a').close()
#
#            found_tasks = set(prj.tasks())
#            self.assertEqual(found_tasks, set('fake_loc/' + v for v in
#                                              scenarios))

    def test_get_exec_cmd(self):
        self.assertEqual('fake_cmd test_item',
                         self.project.get_exec_cmd('test_item'))

    def test_is_task_valid(self):
        with tempfile.TemporaryDirectory() as tmp_dir:

            repo_dir = Path(tmp_dir, 'fake_proj')
            repo_dir.mkdir()

            prj = _project.Project('fake_proj', 'fake_url', 'master',
                                   'ansible', 'fake_loc', 'fake_cmd %%s',
                                   tmp_dir)

            with open(Path(repo_dir, 'test_item'), 'a'):
                self.assertTrue(prj.is_task_valid('test_item'))
                self.assertFalse(prj.is_task_valid('missing_item'))
