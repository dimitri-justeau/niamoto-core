# coding: utf-8

import unittest

from click.testing import CliRunner

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings
from niamoto.bin.commands import raster
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


DB = settings.TEST_DATABASE


class TestCLIDataProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for data provider cli methods.
    """

    def test_list_data_provider_types(self):
        runner = CliRunner()
        result = runner.invoke(raster.list_rasters_cli, ['--database', DB])
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(raster.list_rasters_cli, ['--database', "YO"])
        self.assertEqual(result.exit_code, 1)
        TestDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        result = runner.invoke(raster.list_rasters_cli, ['--database', DB])
        self.assertEqual(result.exit_code, 0)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
