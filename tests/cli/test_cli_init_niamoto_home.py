# coding: utf-8

import unittest
import os
import shutil

from click.testing import CliRunner

from niamoto.testing import set_test_path

set_test_path()

from niamoto import conf
from niamoto.bin.commands.init_niamoto_home import init_niamoto_home_cli
from niamoto.testing.base_tests import BaseTest


class TestCLINiamotoHome(BaseTest):
    """
    Test case for init niamoto home cli method.
    """

    HOME_PATH = os.path.join(conf.NIAMOTO_HOME, "NEW_HOME")

    def tearDown(self):
        shutil.rmtree(self.HOME_PATH)
        set_test_path()

    def test_init_niamoto_home_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            init_niamoto_home_cli,
            ["--niamoto_home_path", conf.NIAMOTO_HOME],
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
