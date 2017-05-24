# coding: utf-8

import unittest

from click.testing import CliRunner

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings
from niamoto.bin.commands.init_db import init_db_cli
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTest


DB = settings.TEST_DATABASE


class TestCLIInitDb(BaseTest):
    """
    Test case for init_db_cli cli method.
    """

    def test_init_db(self):
        runner = CliRunner()
        result = runner.invoke(
            init_db_cli,
            ['--database', DB],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(
            init_db_cli,
            ['--database', None],
        )
        self.assertEqual(result.exit_code, 1)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
