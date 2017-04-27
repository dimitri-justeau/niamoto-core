# coding: utf-8

import unittest

from niamoto.data_providers.occurrence_providers\
    .base_occurrence_provider import *
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto import settings
from tests.test_utils import TestDatabaseManager
from tests import BaseTestNiamotoSchemaCreated
from tests.data_providers.test_base_data_provider import TestDataProvider


class TestBaseOccurrenceProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for base occurrence provider.
    """

    @classmethod
    def setUpClass(cls):
        super(TestBaseOccurrenceProvider, cls).setUpClass()
        TestDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        TestDataProvider.register_data_provider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        TestDataProvider.register_data_provider(
            'test_data_provider_2',
            database=settings.TEST_DATABASE,
        )

    def test_get_current_plot_data(self):
        """
        :return: Test for get_current_plot_data_method.
            Test the structure of the returned DataFrame.
            Test retrieving an empty DataFrame.
            Test retrieving a not-empty DataFrame.
        """
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        data_provider_2 = TestDataProvider(
            'test_data_provider_2',
            database=settings.TEST_DATABASE,
        )
        op1 = BaseOccurrenceProvider(data_provider_1.db_id)
        op2 = BaseOccurrenceProvider(data_provider_2.db_id)
        #  1. retrieve an empty DataFrame
        df1 = op1.get_current_occurrence_data(database=settings.TEST_DATABASE)
        df2 = op2.get_current_occurrence_data(database=settings.TEST_DATABASE)
        self.assertEqual(len(df1), 0)
        self.assertEqual(len(df2), 0)
        #  2. Check the structure of the DataFrame
        df_cols = df1.columns
        db_cols = niamoto_db_meta.occurrence.columns
        for db_col in db_cols:
            self.assertIn(db_col.name, df_cols)
        #  3. Insert some data and test
        ins = niamoto_db_meta.occurrence.insert().values([
            {
                'provider_id': data_provider_1.db_id,
                'provider_pk': 0,
                'location': 'SRID=4326;POINT(166.55121 -22.09739)',
            },
            {
                'provider_id': data_provider_1.db_id,
                'provider_pk': 1,
                'location': 'SRID=4326;POINT(166.551 -22.098)',
            },
            {
                'provider_id': data_provider_2.db_id,
                'provider_pk': 0,
                'location': 'SRID=4326;POINT(166.55121 -22.09739)',
            },
        ])
        with Connector.get_connection(settings.TEST_DATABASE) as connection:
            connection.execute(ins)
            df3 = op1.get_current_occurrence_data(
                database=settings.TEST_DATABASE
            )
            df4 = op2.get_current_occurrence_data(
                database=settings.TEST_DATABASE
            )
            self.assertEqual(len(df3), 2)
            self.assertEqual(len(df4), 1)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
