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

from unittest import TestCase
import mock

from git import Repo
import tempfile
from pathlib import Path

from apimon import project as _project


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

            with mock.patch.object(prj, 'refresh_git_repo', return_value=True):
                prj.get_git_repo()

    def test_get_git_repo_fresh(self):
        with mock.patch.object(Repo, 'clone_from') as git_mock:
            repo = mock.Mock(autospec=Repo)
            git_mock.return_value = repo

            self.project.get_git_repo()

            git_mock.assert_called_with('fake_url',
                                        Path('wrk_dir', 'fake_proj'),
                                        recurse_submodules='.')
            repo.remotes.origin.pull.assert_called_with('master')

    def test_refresh_git_repo(self):
        with mock.patch.object(self.project, 'repo') as repo_mock:
            self.assertIsNone(self.project.refresh_git_repo())
            repo_mock.remotes.origin.pull.assert_called_with(
                'master',
                recurse_submodules=True)

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

            with mock.patch.object(prj, 'refresh_git_repo'), \
                    mock.patch.object(prj, '_ansible_galaxy_install') \
                    as install_mock:
                prj.prepare()
                install_mock.assert_not_called()

                # Now write file and ensure galaxy invoked
                open(requirements_file, 'a').close()
                prj.prepare()
                calls = [
                    mock.call('role', requirements_file),
                    mock.call('collection', requirements_file)
                ]
                install_mock.assert_has_calls(calls)

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
