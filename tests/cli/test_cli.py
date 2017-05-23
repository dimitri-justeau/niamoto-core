# coding: utf-8

# coding: utf-8

import unittest

from click.testing import CliRunner

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings
from niamoto.bin.cli import niamoto_cli
from niamoto.testing.base_tests import BaseTest

settings.DEFAULT_DATABASE = settings.TEST_DATABASE

DB = settings.TEST_DATABASE


class TestCLI(BaseTest):
    """
    Test case for main cli method.
    """

    def test_cli(self):
        runner = CliRunner()
        result = runner.invoke(niamoto_cli, [])
        assert result.exit_code == 0


if __name__ == '__main__':
    unittest.main()
