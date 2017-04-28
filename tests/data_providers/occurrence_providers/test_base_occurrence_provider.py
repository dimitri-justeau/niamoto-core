# coding: utf-8

import unittest

import pandas as pd
from shapely.geometry import Point
from geoalchemy2.shape import from_shape

from niamoto.data_providers.occurrence_providers\
    .base_occurrence_provider import *
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_data_provider import TestDataProvider


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
        #  3. Insert some data and test
        occ_1 = [
            {
                'provider_id': data_provider_1.db_id,
                'provider_pk': 0,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
            },
            {
                'provider_id': data_provider_1.db_id,
                'provider_pk': 1,
                'location': from_shape(Point(166.551, -22.098), srid=4326),
            },
            {
                'provider_id': data_provider_1.db_id,
                'provider_pk': 2,
                'location': from_shape(Point(166.552, -22.097), srid=4326),
            },
            {
                'provider_id': data_provider_1.db_id,
                'provider_pk': 3,
                'location': from_shape(Point(166.553, -22.099), srid=4326),
            },
        ]
        occ_2 = [
            {
                'provider_id': data_provider_2.db_id,
                'provider_pk': 0,
                'location': from_shape(Point(166.5511, -22.09739), srid=4326),
            },
        ]
        ins = niamoto_db_meta.occurrence.insert().values(occ_1 + occ_2)
        with Connector.get_connection(settings.TEST_DATABASE) as connection:
            connection.execute(ins)

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
        data_provider_3 = TestDataProvider(
            'test_data_provider_3',
            database=settings.TEST_DATABASE,
        )
        op1 = BaseOccurrenceProvider(data_provider_1)
        op2 = BaseOccurrenceProvider(data_provider_2)
        op3 = BaseOccurrenceProvider(data_provider_3)
        #  1. retrieve DataFrames
        df1 = op1.get_niamoto_occurrence_dataframe(
            database=settings.TEST_DATABASE
        )
        df2 = op2.get_niamoto_occurrence_dataframe(
            database=settings.TEST_DATABASE
        )
        df3 = op3.get_niamoto_occurrence_dataframe(
            database=settings.TEST_DATABASE
        )
        self.assertEqual(len(df1), 4)
        self.assertEqual(len(df2), 1)
        self.assertEqual(len(df3), 0)
        #  2. Check the structure of the DataFrame
        df_cols = list(df1.columns) + [df1.index.name, ]
        db_cols = niamoto_db_meta.occurrence.columns
        for db_col in db_cols:
            self.assertIn(db_col.name, df_cols)

    def test_get_insert_dataframe(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        op1 = BaseOccurrenceProvider(data_provider_1)
        df1 = op1.get_niamoto_occurrence_dataframe(
            database=settings.TEST_DATABASE
        )
        #  1. Nothing to insert
        occ_1 = pd.DataFrame.from_records([
            {
                'id': 0,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
            },
        ], index='id')
        insert = op1.get_insert_dataframe(df1, occ_1)
        self.assertEqual(len(insert), 0)
        # 2. Everything to insert
        occ_2 = pd.DataFrame.from_records([
            {
                'id': 10,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
            },
            {
                'id': 11,
                'location': from_shape(Point(166.551, -22.098), srid=4326),
            },
        ], index='id')
        insert = op1.get_insert_dataframe(df1, occ_2)
        self.assertEqual(len(insert), 2)
        # 3. Partial insert
        occ_3 = pd.DataFrame.from_records([
            {
                'id': 0,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
            },
            {
                'id': 11,
                'location': from_shape(Point(166.551, -22.098), srid=4326),
            },
        ], index='id')
        insert = op1.get_insert_dataframe(df1, occ_3)
        self.assertEqual(len(insert), 1)

    def test_get_update_dataframe(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        op1 = BaseOccurrenceProvider(data_provider_1)
        df1 = op1.get_niamoto_occurrence_dataframe(
            database=settings.TEST_DATABASE
        )
        #  1. Nothing to update
        #  2. Everything to update
        #  3. Partial update

    def test_get_delete_dataframe(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        op1 = BaseOccurrenceProvider(data_provider_1)
        df1 = op1.get_niamoto_occurrence_dataframe(
            database=settings.TEST_DATABASE
        )
        #  1. Nothing to delete
        #  2. Everything to delete
        #  3. Partial delete

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
