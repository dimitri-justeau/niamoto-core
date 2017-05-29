# coding: utf-8

import unittest

from click.testing import CliRunner

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.bin.commands import data_provider
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


DB = settings.TEST_DATABASE


class TestCLIDataProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for data provider cli methods.
    """

    def tearDown(self):
        del1 = niamoto_db_meta.data_provider.delete()
        del2 = niamoto_db_meta.data_provider_type.delete()
        with Connector.get_connection(database=DB) as connection:
            connection.execute(del1)
            connection.execute(del2)
            TestDataProvider._unregister_unique_synonym_constraint(connection)

    def test_list_data_provider_types(self):
        runner = CliRunner()
        result = runner.invoke(
            data_provider.list_data_provider_types,
            ['--database', DB]
        )
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(
            data_provider.list_data_provider_types,
            ['--database', "YO"]
        )
        self.assertEqual(result.exit_code, 1)
        TestDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        result = runner.invoke(
            data_provider.list_data_provider_types,
            ['--database', DB]
        )
        self.assertEqual(result.exit_code, 0)

    def test_list_data_providers(self):
        runner = CliRunner()
        result = runner.invoke(
            data_provider.list_data_providers,
            ['--database', DB]
        )
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(
            data_provider.list_data_providers,
            ['--database', "YO"]
        )
        self.assertEqual(result.exit_code, 1)
        TestDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        TestDataProvider.register_data_provider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE
        )
        result = runner.invoke(
            data_provider.list_data_providers,
            ['--database', DB]
        )
        self.assertEqual(result.exit_code, 0)

    def test_add_data_provider(self):
        runner = CliRunner()
        TestDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        PlantnoteDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        result = runner.invoke(
            data_provider.add_data_provider,
            ['--database', DB, 'test_provider', 'PLANTNOTE'],
        )
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(
            data_provider.add_data_provider,
            ['--database', DB, 'test_provider', 'PLANTNOTE'],
        )
        self.assertEqual(result.exit_code, 1)
        result = runner.invoke(
            data_provider.add_data_provider,
            ['--database', 'YO', 'test_provider', 'PLANTNOTE'],
        )
        self.assertEqual(result.exit_code, 1)

    def test_delete_data_provider(self):
        runner = CliRunner()
        TestDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        TestDataProvider.register_data_provider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE
        )
        result = runner.invoke(
            data_provider.delete_data_provider,
            ['--database', DB, 'test_data_provider_1']
        )
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(
            data_provider.delete_data_provider,
            ['--database', DB, 'test_data_provider_1']
        )
        self.assertEqual(result.exit_code, 1)
        result = runner.invoke(
            data_provider.delete_data_provider,
            ['--database', 'YO', 'test_data_provider_1']
        )
        self.assertEqual(result.exit_code, 1)

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
