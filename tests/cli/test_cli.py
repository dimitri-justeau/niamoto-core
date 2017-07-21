# coding: utf-8

# coding: utf-8

import unittest

from click.testing import CliRunner
import click

from niamoto.testing import set_test_path

set_test_path()

from niamoto.bin.cli import niamoto_cli
from niamoto.testing.base_tests import BaseTest


class TestCLI(BaseTest):
    """
    Test case for main cli method.
    """

    def test_cli(self):
        @niamoto_cli.command()
        def foo():
            pass
        runner = CliRunner()
        result = runner.invoke(
            niamoto_cli,
            ['foo']
        )
        assert result.exit_code == 0
        result = runner.invoke(
            niamoto_cli,
            []
        )
        assert result.exit_code == 0


if __name__ == '__main__':
    unittest.main()
