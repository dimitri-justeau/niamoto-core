# coding: utf-8

import unittest

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings
from niamoto.data_providers.base_plot_occurrence_provider import *
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.db.utils import fix_db_sequences
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing import test_data


class TestBasePlotProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for base plot provider.
    """

    @classmethod
    def setUpClass(cls):
        super(TestBasePlotProvider, cls).setUpClass()
        TestDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        data_provider_1 = TestDataProvider.register_data_provider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        data_provider_2 = TestDataProvider.register_data_provider(
            'test_data_provider_2',
            database=settings.TEST_DATABASE,
        )
        TestDataProvider.register_data_provider(
            'test_data_provider_3',
            database=settings.TEST_DATABASE,
        )
        plot_1 = test_data.get_plot_data_1(data_provider_1)
        plot_2 = test_data.get_plot_data_2(data_provider_2)
        occ_1 = test_data.get_occurrence_data_1(data_provider_1)
        occ_2 = test_data.get_occurrence_data_2(data_provider_2)
        plot_occ_1 = test_data.get_plot_occurrence_data_1(data_provider_1)
        plot_occ_2 = test_data.get_plot_occurrence_data_2(data_provider_2)
        ins_1 = niamoto_db_meta.plot.insert().values(plot_1 + plot_2)
        ins_2 = niamoto_db_meta.occurrence.insert().values(occ_1 + occ_2)
        ins_3 = niamoto_db_meta.plot_occurrence.insert().values(
            plot_occ_1 + plot_occ_2
        )
        with Connector.get_connection(settings.TEST_DATABASE) as connection:
            connection.execute(ins_1)
            connection.execute(ins_2)
            connection.execute(ins_3)
        fix_db_sequences(database=settings.TEST_DATABASE)

    def test_get_current_plot_occ_data(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        data_provider_2 = TestDataProvider(
            'test_data_provider_2',
            database=settings.TEST_DATABASE,
        )
        data_provider_3 = TestDataProvider(
            'test_data_provider_3',
            database=settings.TEST_DATABASE,
        )
        prov1 = BasePlotOccurrenceProvider(data_provider_1)
        prov2 = BasePlotOccurrenceProvider(data_provider_2)
        prov3 = BasePlotOccurrenceProvider(data_provider_3)
        df1 = prov1.get_niamoto_plot_occurrence_dataframe()
        df2 = prov2.get_niamoto_plot_occurrence_dataframe()
        df3 = prov3.get_niamoto_plot_occurrence_dataframe()
        self.assertEqual(len(df1), 5)
        self.assertEqual(len(df2), 1)
        self.assertEqual(len(df3), 0)

    def test_get_insert_dataframe(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )

    def test_get_update_dataframe(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )

    def test_get_delete_dataframe(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )

    def test_sync_insert(self):
        self.tearDownClass()
        self.setUpClass()

    def test_sync_update(self):
        self.tearDownClass()
        self.setUpClass()

    def test_sync_delete(self):
        self.tearDownClass()
        self.setUpClass()

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

