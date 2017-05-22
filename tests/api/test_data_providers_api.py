# coding: utf-8

import unittest
import os

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.api.data_providers import *
from niamoto.db import metadata as niamoto_db_meta
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider


DB = settings.TEST_DATABASE


class TestDataProvidersApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for data providers api.
    """

    TEST_DB_PATH = os.path.join(
        NIAMOTO_HOME,
        'data',
        'plantnote',
        'ncpippn_test.sqlite',
    )

    @classmethod
    def setUpClass(cls):
        super(TestDataProvidersApi, cls).setUpClass()

    def tearDown(self):
        with Connector.get_connection(database=DB) as connection:
            del1 = niamoto_db_meta.data_provider.delete()
            del2 = niamoto_db_meta.data_provider_type.delete()
            connection.execute(del1)
            connection.execute(del2)
        TestDataProvider._unregister_unique_synonym_constraint(database=DB)
        PlantnoteDataProvider._unregister_unique_synonym_constraint(
            database=DB
        )

    def test_get_data_provider_type_list(self):
        l1 = get_data_provider_type_list(database=DB)
        self.assertEqual(len(l1), 0)
        PlantnoteDataProvider.register_data_provider_type(database=DB)
        TestDataProvider.register_data_provider_type(database=DB)
        l2 = get_data_provider_type_list(database=DB)
        self.assertEqual(len(l2), 2)

    def test_get_data_provider_list(self):
        PlantnoteDataProvider.register_data_provider_type(database=DB)
        TestDataProvider.register_data_provider_type(database=DB)
        l1 = get_data_provider_list(database=DB)
        self.assertEqual(len(l1), 0)
        TestDataProvider.register_data_provider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE
        )
        PlantnoteDataProvider.register_data_provider(
            'pl@ntnote_provider_1',
            self.TEST_DB_PATH,
            database=settings.TEST_DATABASE
        )
        PlantnoteDataProvider.register_data_provider(
            'pl@ntnote_provider_2',
            self.TEST_DB_PATH,
            database=settings.TEST_DATABASE
        )
        l2 = get_data_provider_list(database=DB)
        self.assertEqual(len(l2), 3)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
