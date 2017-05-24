# coding: utf-8

import unittest
import os
import shutil

from click.testing import CliRunner

from niamoto.testing import set_test_path, TEST_PATH

set_test_path()

from niamoto.bin.commands.init_niamoto_home import init_niamoto_home_cli
from niamoto.testing.base_tests import BaseTest


class TestCLIInitDb(BaseTest):
    """
    Test case for init_db_cli cli method.
    """

    HOME_PATH = os.path.join(TEST_PATH, "NEW_HOME")

    def tearDown(self):
        shutil.rmtree(self.HOME_PATH)

    def test_init_niamoto_home_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            init_niamoto_home_cli,
            ["--niamoto_home_path", TEST_PATH],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 1)
        result = runner.invoke(
            init_niamoto_home_cli,
            ["--niamoto_home_path", self.HOME_PATH],
            catch_exceptions=False,
        )
        assert result.exit_code == 0


if __name__ == '__main__':
    unittest.main()
