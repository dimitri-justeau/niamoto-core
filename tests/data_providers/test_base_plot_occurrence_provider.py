# coding: utf-8

import unittest

import pandas as pd

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings
from niamoto.data_providers.base_plot_occurrence_provider \
    import BasePlotOccurrenceProvider
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

    def test_get_niamoto_index(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        prov1 = BasePlotOccurrenceProvider(data_provider_1)
        data = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 1,
                'occurrence_identifier': 'PLOT1_001',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 2,
                'occurrence_identifier': 'PLOT1_002',
            },
            {
                'provider_plot_pk': 2,
                'provider_occurrence_pk': 5,
                'occurrence_identifier': 'PLOT2_002',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        reindexed = prov1.get_reindexed_provider_dataframe(data)
        self.assertEqual(
            list(reindexed.index.get_values()),
            [(1, 1), (1, 2), (2, 3), ],
        )

    def test_get_insert_dataframe(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        prov1 = BasePlotOccurrenceProvider(data_provider_1)
        df1 = prov1.get_niamoto_plot_occurrence_dataframe()
        #  1. Nothing to insert
        data_1 = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 1,
                'occurrence_identifier': 'PLOT2_001',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        reindexed_data_1 = prov1.get_reindexed_provider_dataframe(data_1)
        ins = prov1.get_insert_dataframe(df1, reindexed_data_1)
        self.assertEqual(len(ins), 0)
        # 2. Everything to insert
        data_2 = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 1,
                'occurrence_identifier': 'PLOT1_002',
            },
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 2,
                'occurrence_identifier': 'PLOT1_003',
            },
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 5,
                'occurrence_identifier': 'PLOT1_003',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        reindexed_data_2 = prov1.get_reindexed_provider_dataframe(data_2)
        ins = prov1.get_insert_dataframe(df1, reindexed_data_2)
        self.assertEqual(len(ins), 3)
        # 3. Partial insert
        data_3 = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'PLOT1_001',
            },
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 2,
                'occurrence_identifier': 'PLOT1_003',
            },
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 5,
                'occurrence_identifier': 'PLOT1_003',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        reindexed_data_3 = prov1.get_reindexed_provider_dataframe(data_3)
        ins = prov1.get_insert_dataframe(df1, reindexed_data_3)
        self.assertEqual(len(ins), 2)
        self.assertEqual(list(ins['provider_occurrence_pk']), [2, 5])

    def test_get_update_dataframe(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        prov1 = BasePlotOccurrenceProvider(data_provider_1)
        df1 = prov1.get_niamoto_plot_occurrence_dataframe()
        #  1. Nothing to update
        data_1 = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 1,
                'occurrence_identifier': 'PLOT1_002',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        reindexed_data_1 = prov1.get_reindexed_provider_dataframe(data_1)
        upd = prov1.get_update_dataframe(df1, reindexed_data_1)
        self.assertEqual(len(upd), 0)
        # 2. Everything to update
        data_2 = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'PLOT1_002',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 2,
                'occurrence_identifier': 'PLOT1_003',
            },
            {
                'provider_plot_pk': 2,
                'provider_occurrence_pk': 5,
                'occurrence_identifier': 'PLOT1_003',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        reindexed_data_2 = prov1.get_reindexed_provider_dataframe(data_2)
        upd = prov1.get_update_dataframe(df1, reindexed_data_2)
        self.assertEqual(len(upd), 3)
        # 3. Partial update
        data_3 = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'PLOT1_002',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 2,
                'occurrence_identifier': 'PLOT1_003',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 5,
                'occurrence_identifier': 'PLOT1_003',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        reindexed_data_3 = prov1.get_reindexed_provider_dataframe(data_3)
        upd = prov1.get_update_dataframe(df1, reindexed_data_3)
        self.assertEqual(len(upd), 2)
        self.assertEqual(list(upd['provider_occurrence_pk']), [0, 2])

    def test_get_delete_dataframe(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        prov1 = BasePlotOccurrenceProvider(data_provider_1)
        df1 = prov1.get_niamoto_plot_occurrence_dataframe()
        #  1. Nothing to delete
        data_1 = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'PLOT1_000',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'PLOT2_000',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 1,
                'occurrence_identifier': 'PLOT2_001',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 2,
                'occurrence_identifier': 'PLOT2_002',
            },
            {
                'provider_plot_pk': 2,
                'provider_occurrence_pk': 5,
                'occurrence_identifier': 'PLOT2_002',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        reindexed_data_1 = prov1.get_reindexed_provider_dataframe(data_1)
        delete = prov1.get_delete_dataframe(df1, reindexed_data_1)
        self.assertEqual(len(delete), 0)
        # 2. Everything to delete
        data_2 = pd.DataFrame.from_records([
        ])
        reindexed_data_2 = prov1.get_reindexed_provider_dataframe(data_2)
        delete = prov1.get_delete_dataframe(df1, reindexed_data_2)
        self.assertEqual(len(delete), 5)
        # 3. Partial delete
        data_3 = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'PLOT1_000',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'PLOT2_000',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        reindexed_data_3 = prov1.get_reindexed_provider_dataframe(data_3)
        delete = prov1.get_delete_dataframe(df1, reindexed_data_3)
        self.assertEqual(len(delete), 3)
        self.assertEqual(list(delete['provider_occurrence_pk']), [1, 2, 5])

    def test_sync_insert(self):
        self.tearDownClass()
        super(TestBasePlotProvider, self).setUpClass()
        # Reset the data
        TestDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        data_provider_1 = TestDataProvider.register_data_provider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        plot_1 = test_data.get_plot_data_1(data_provider_1)
        occ_1 = test_data.get_occurrence_data_1(data_provider_1)
        ins_1 = niamoto_db_meta.plot.insert().values(plot_1)
        ins_2 = niamoto_db_meta.occurrence.insert().values(occ_1)
        with Connector.get_connection(settings.TEST_DATABASE) as connection:
            connection.execute(ins_1)
            connection.execute(ins_2)
        fix_db_sequences(database=settings.TEST_DATABASE)
        # Test
        prov = BasePlotOccurrenceProvider(data_provider_1)
        self.assertEqual(len(prov.get_niamoto_plot_occurrence_dataframe()), 0)
        data = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'TEST',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 1,
                'occurrence_identifier': 'TEST_ENCORE',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        data = prov.get_reindexed_provider_dataframe(data)
        i, u, d = prov._sync(data)
        self.assertEqual(len(i), 2)
        self.assertEqual(len(u), 0)
        self.assertEqual(len(d), 0)
        self.assertEqual(len(prov.get_niamoto_plot_occurrence_dataframe()), 2)

    def test_sync_update(self):
        self.tearDownClass()
        self.setUpClass()
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        prov = BasePlotOccurrenceProvider(data_provider_1)
        self.assertEqual(len(prov.get_niamoto_plot_occurrence_dataframe()), 5)
        data = pd.DataFrame.from_records([
            {
                'provider_plot_pk': 0,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'PLOT1_000',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 0,
                'occurrence_identifier': 'PLOT2_000',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 1,
                'occurrence_identifier': 'PLOT2_001',
            },
            {
                'provider_plot_pk': 1,
                'provider_occurrence_pk': 2,
                'occurrence_identifier': 'PLOT2_002',
            },
            {
                'provider_plot_pk': 2,
                'provider_occurrence_pk': 5,
                'occurrence_identifier': 'PLOT2_002',
            },
        ], index=['provider_plot_pk', 'provider_occurrence_pk'])
        data = prov.get_reindexed_provider_dataframe(data)
        i, u, d = prov._sync(data)
        self.assertEqual(len(i), 0)
        self.assertEqual(len(u), 5)
        self.assertEqual(len(d), 0)
        self.assertEqual(len(prov.get_niamoto_plot_occurrence_dataframe()), 5)

    def test_sync_delete(self):
        self.tearDownClass()
        self.setUpClass()
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        prov = BasePlotOccurrenceProvider(data_provider_1)
        self.assertEqual(len(prov.get_niamoto_plot_occurrence_dataframe()), 5)
        data = pd.DataFrame.from_records([])
        data = prov.get_reindexed_provider_dataframe(data)
        i, u, d = prov._sync(data)
        self.assertEqual(len(i), 0)
        self.assertEqual(len(u), 0)
        self.assertEqual(len(d), 5)
        self.assertEqual(len(prov.get_niamoto_plot_occurrence_dataframe()), 0)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

