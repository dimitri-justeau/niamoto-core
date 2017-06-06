# coding: utf-8

import unittest
import os

from geoalchemy2.shape import from_shape, WKTElement
from shapely.geometry import Point
import numpy as np

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.data_providers.base_occurrence_provider import *
from niamoto.api import taxonomy_api
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.db.utils import fix_db_sequences
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing import test_data


class TestBaseOccurrenceProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for base occurrence provider.
    """

    @classmethod
    def setUpClass(cls):
        super(TestBaseOccurrenceProvider, cls).setUpClass()
        TestDataProvider.register_data_provider_type()
        data_provider_1 = TestDataProvider.register_data_provider(
            'test_data_provider_1',
        )
        data_provider_2 = TestDataProvider.register_data_provider(
            'test_data_provider_2',
        )
        TestDataProvider.register_data_provider(
            'test_data_provider_3',
        )
        occ_1 = test_data.get_occurrence_data_1(data_provider_1)
        occ_2 = test_data.get_occurrence_data_2(data_provider_2)
        ins = niamoto_db_meta.occurrence.insert().values(occ_1 + occ_2)
        with Connector.get_connection() as connection:
            connection.execute(ins)
        fix_db_sequences()

    def test_get_current_plot_data(self):
        """
        :return: Test for get_current_occurrence_data_method.
            Test the structure of the returned DataFrame.
            Test retrieving an empty DataFrame.
            Test retrieving a not-empty DataFrame.
        """
        data_provider_1 = TestDataProvider('test_data_provider_1')
        data_provider_2 = TestDataProvider('test_data_provider_2')
        data_provider_3 = TestDataProvider('test_data_provider_3')
        with Connector.get_connection() as connection:
            op1 = BaseOccurrenceProvider(data_provider_1)
            op2 = BaseOccurrenceProvider(data_provider_2)
            op3 = BaseOccurrenceProvider(data_provider_3)
            #  1. retrieve DataFrames
            df1 = op1.get_niamoto_occurrence_dataframe(connection)
            df2 = op2.get_niamoto_occurrence_dataframe(connection)
            df3 = op3.get_niamoto_occurrence_dataframe(connection)
            self.assertEqual(len(df1), 4)
            self.assertEqual(len(df2), 1)
            self.assertEqual(len(df3), 0)
            #  2. Check the structure of the DataFrame
            df_cols = list(df1.columns) + [df1.index.name, ]
            db_cols = niamoto_db_meta.occurrence.columns
            for db_col in db_cols:
                self.assertIn(db_col.name, df_cols)

    def test_get_insert_dataframe(self):
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            op1 = BaseOccurrenceProvider(data_provider_1)
            df1 = op1.get_niamoto_occurrence_dataframe(connection)
        #  1. Nothing to insert
        occ_1 = pd.DataFrame.from_records([
            {
                'id': 0,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        ins = op1.get_insert_dataframe(df1, occ_1)
        self.assertEqual(len(ins), 0)
        # 2. Everything to insert
        occ_2 = pd.DataFrame.from_records([
            {
                'id': 10,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 11,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        ins = op1.get_insert_dataframe(df1, occ_2)
        self.assertIn('provider_pk', ins.columns)
        self.assertIn('provider_id', ins.columns)
        self.assertEqual(len(ins[pd.isnull(ins['provider_pk'])]), 0)
        self.assertEqual(len(ins[pd.isnull(ins['provider_id'])]), 0)
        self.assertEqual(len(ins), 2)
        # 3. Partial insert
        occ_3 = pd.DataFrame.from_records([
            {
                'id': 0,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 11,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        ins = op1.get_insert_dataframe(df1, occ_3)
        self.assertEqual(len(ins), 1)

    def test_get_update_dataframe(self):
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            op1 = BaseOccurrenceProvider(data_provider_1)
            df1 = op1.get_niamoto_occurrence_dataframe(connection)
        #  1. Nothing to update
        occ_1 = pd.DataFrame.from_records([
            {
                'id': 10,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        update_df = op1.get_update_dataframe(df1, occ_1)
        self.assertEqual(len(update_df), 0)
        #  2. Everything to update
        occ_2 = pd.DataFrame.from_records([
            {
                'id': 0,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 1,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
            {
                'id': 2,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.552, -22.097), srid=4326),
                'properties': '{}',
            },
            {
                'id': 5,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.553, -22.099), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        update_df = op1.get_update_dataframe(df1, occ_2)
        self.assertIn('provider_pk', update_df.columns)
        self.assertIn('provider_id', update_df.columns)
        self.assertEqual(
            len(update_df[pd.isnull(update_df['provider_pk'])]),
            0
        )
        self.assertEqual(
            len(update_df[pd.isnull(update_df['provider_id'])]),
            0
        )
        self.assertEqual(len(update_df), 4)
        #  3. Partial update
        occ_3 = pd.DataFrame.from_records([
            {
                'id': 0,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 1,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        update_df = op1.get_update_dataframe(df1, occ_3)
        self.assertEqual(len(update_df), 2)

    def test_get_delete_dataframe(self):
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            op1 = BaseOccurrenceProvider(data_provider_1)
            df1 = op1.get_niamoto_occurrence_dataframe(connection)
        #  1. Nothing to delete
        occ_1 = pd.DataFrame.from_records([
            {
                'id': 0,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 1,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
            {
                'id': 2,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.552, -22.097), srid=4326),
                'properties': '{}',
            },
            {
                'id': 5,
                'taxon_id': None,
                'provider_taxon_id': None,
                'location': from_shape(Point(166.553, -22.099), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        delete_df = op1.get_delete_dataframe(df1, occ_1)
        self.assertIn('provider_pk', delete_df.columns)
        self.assertIn('provider_id', delete_df.columns)
        self.assertEqual(
            len(delete_df[pd.isnull(delete_df['provider_pk'])]),
            0
        )
        self.assertEqual(
            len(delete_df[pd.isnull(delete_df['provider_id'])]),
            0
        )
        self.assertEqual(len(delete_df), 0)
        self.assertEqual(len(delete_df[pd.isnull(delete_df['location'])]), 0)
        #  2. Everything to delete
        occ_2 = pd.DataFrame.from_records([
            {
                'id': 10,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        delete_df = op1.get_delete_dataframe(df1, occ_2)

        self.assertEqual(len(delete_df), 4)
        self.assertEqual(len(delete_df[pd.isnull(delete_df['location'])]), 0)
        #  3. Partial delete
        occ_3 = pd.DataFrame.from_records([
            {
                'id': 0,
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        delete_df = op1.get_delete_dataframe(df1, occ_3)
        self.assertEqual(len(delete_df), 3)
        self.assertEqual(len(delete_df[pd.isnull(delete_df['location'])]), 0)

    def test_sync_insert(self):
        self.tearDownClass()
        self.setUpClass()
        data_provider_3 = TestDataProvider('test_data_provider_3')
        with Connector.get_connection() as connection:
            op3 = BaseOccurrenceProvider(data_provider_3)
            self.assertEqual(
                len(op3.get_niamoto_occurrence_dataframe(connection)),
                0
            )
            occ = pd.DataFrame.from_records([
                {
                    'id': 0,
                    'taxon_id': None,
                    'provider_taxon_id': None,
                    'location': from_shape(Point(166.551, -22.039), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 1,
                    'taxon_id': None,
                    'provider_taxon_id': None,
                    'location': from_shape(Point(166.551, -22.098), srid=4326),
                    'properties': '{}',
                },
            ], index='id')
            i, u, d = op3._sync(occ, connection)
            self.assertEqual(len(i), 2)
            self.assertEqual(len(u), 0)
            self.assertEqual(len(d), 0)
            self.assertEqual(
                len(op3.get_niamoto_occurrence_dataframe(connection)),
                2
            )

    def test_sync_update(self):
        self.tearDownClass()
        self.setUpClass()
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            op1 = BaseOccurrenceProvider(data_provider_1)
            self.assertEqual(
                len(op1.get_niamoto_occurrence_dataframe(connection)),
                4
            )
            occ = pd.DataFrame.from_records([
                {
                    'id': 0,
                    'taxon_id': None,
                    'provider_taxon_id': None,
                    'location': WKTElement(
                        Point(166.5521, -22.0939).wkt,
                        srid=4326
                    ),
                    'properties': '{}',
                },
                {
                    'id': 1,
                    'taxon_id': None,
                    'provider_taxon_id': None,
                    'location': from_shape(Point(166.551, -22.098), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 2,
                    'taxon_id': None,
                    'provider_taxon_id': None,
                    'properties': '{"yo": "yo"}',
                    'location': from_shape(Point(166.552, -22.097), srid=4326)
                },
                {
                    'id': 5,
                    'taxon_id': None,
                    'provider_taxon_id': None,
                    'properties': '{}',
                    'location': WKTElement(
                        Point(166.553, -22.099),
                        srid=4326
                    ),
                },
            ], index='id')
            i, u, d = op1._sync(occ, connection)
            self.assertEqual(len(i), 0)
            self.assertEqual(len(u), 4)
            self.assertEqual(len(d), 0)
            self.assertEqual(
                len(op1.get_niamoto_occurrence_dataframe(connection)),
                4
            )

    def test_sync_delete(self):
        self.tearDownClass()
        self.setUpClass()
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            op1 = BaseOccurrenceProvider(data_provider_1)
            self.assertEqual(
                len(op1.get_niamoto_occurrence_dataframe(connection)),
                4
            )
            occ = pd.DataFrame.from_records(
                [],
                index='id',
                columns=('id', 'location')
            )
            i, u, d = op1._sync(occ, connection)
            self.assertEqual(len(i), 0)
            self.assertEqual(len(u), 0)
            self.assertEqual(len(d), 4)
            self.assertEqual(
                len(op1.get_niamoto_occurrence_dataframe(connection)),
                0
            )

    def test_update_synonym_mapping(self):
        self.tearDownClass()
        self.setUpClass()
        data_provider_3 = TestDataProvider('test_data_provider_3')
        with Connector.get_connection() as connection:
            op3 = BaseOccurrenceProvider(data_provider_3)
            occ = pd.DataFrame.from_records([
                {
                    'id': 0,
                    'taxon_id': None,
                    'provider_taxon_id': 20,
                    'location': from_shape(Point(166.551, -22.039), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 1,
                    'taxon_id': None,
                    'provider_taxon_id': 30,
                    'location': from_shape(Point(166.551, -22.098), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 2,
                    'taxon_id': None,
                    'provider_taxon_id': 60,
                    'location': from_shape(Point(166.551, -22.098), srid=4326),
                    'properties': '{}',
                },
            ], index='id')
            op3._sync(occ, connection)
            taxonomy_csv_path = os.path.join(
                NIAMOTO_HOME,
                'data',
                'taxonomy',
                'taxonomy_1.csv',
            )
            taxonomy_api.set_taxonomy(taxonomy_csv_path)
            data_provider_3 = TestDataProvider.update_data_provider(
                "test_data_provider_3",
                "test_data_provider_3",
                synonym_key='gbif'
            )
            op3 = BaseOccurrenceProvider(data_provider_3)
            op3.update_synonym_mapping(connection)
            df = op3.get_niamoto_occurrence_dataframe(connection)
            r1 = df[df['taxon_id'].isnull()]
            self.assertEqual(len(r1), 1)
            self.assertEqual(r1.iloc[0]['provider_taxon_id'], 60)
            r2 = df[~df['taxon_id'].isnull()]
            self.assertEqual(len(r2), 2)
            self.assertEqual(set(r2['taxon_id']), {1, 2})
            self.assertEqual(
                r2[r2['taxon_id'] == 1]['provider_taxon_id'].iloc[0],
                20,
            )
            self.assertEqual(
                r2[r2['taxon_id'] == 2]['provider_taxon_id'].iloc[0],
                30,
            )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
