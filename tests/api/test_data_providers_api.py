# coding: utf-8

import unittest
import os

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import NIAMOTO_HOME
from niamoto.conf import settings
from niamoto.api.data_provider_api import *
from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.db import metadata as niamoto_db_meta
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider
from niamoto.exceptions import NoRecordFoundError


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
        with Connector.get_connection() as connection:
            del1 = niamoto_db_meta.data_provider.delete()
            del3 = niamoto_db_meta.occurrence.delete()
            del4 = niamoto_db_meta.plot.delete()
            del5 = niamoto_db_meta.plot_occurrence.delete()
            connection.execute(del5)
            connection.execute(del3)
            connection.execute(del4)
            connection.execute(del1)

    def test_get_data_provider_type_list(self):
        get_data_provider_type_list()

    def test_get_data_provider_list(self):
        l1 = get_data_provider_list()
        self.assertEqual(len(l1), 0)
        TestDataProvider.register_data_provider('test_data_provider_1')
        PlantnoteDataProvider.register_data_provider(
            'pl@ntnote_provider_1',
            self.TEST_DB_PATH,
        )
        PlantnoteDataProvider.register_data_provider(
            'pl@ntnote_provider_2',
            self.TEST_DB_PATH,
        )
        l2 = get_data_provider_list()
        self.assertEqual(len(l2), 3)

    def test_add_data_provider(self):
        l1 = get_data_provider_list()
        self.assertEqual(len(l1), 0)
        add_data_provider(
            "pl@ntnote_provider_1",
            "PLANTNOTE",
        )
        l2 = get_data_provider_list()
        self.assertEqual(len(l2), 1)

    def test_update_data_provider(self):
        add_data_provider(
            "pl@ntnote_provider_1",
            "PLANTNOTE",
        )
        TaxonomyManager.register_synonym_key("YO")
        update_data_provider(
            "pl@ntnote_provider_1",
            new_name="pl@ntnote",
            synonym_key="YO"
        )
        l2 = get_data_provider_list()
        self.assertIn("pl@ntnote", list(l2['name']))
        self.assertIn("YO", list(l2['synonym_key']))

    def test_delete_data_provider(self):
        add_data_provider(
            "pl@ntnote_provider_1",
            "PLANTNOTE",
        )
        l1 = get_data_provider_list()
        self.assertEqual(len(l1), 1)
        delete_data_provider(
            "pl@ntnote_provider_1",
        )
        l1 = get_data_provider_list()
        self.assertEqual(len(l1), 0)
        self.assertRaises(
            NoRecordFoundError,
            delete_data_provider,
            "pl@ntnote_provider_1",
        )

    def test_sync_with_data_provider(self):
        add_data_provider(
            "pl@ntnote_provider_1",
            "PLANTNOTE",
        )
        sync_with_data_provider(
            "pl@ntnote_provider_1",
            self.TEST_DB_PATH,
        )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
