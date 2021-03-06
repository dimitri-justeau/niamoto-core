# coding: utf-8

import unittest

from geoalchemy2.shape import from_shape, WKTElement
from shapely.geometry import Point

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings
from niamoto.data_providers.base_plot_provider import *
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
        data_provider_1 = TestDataProvider.register_data_provider(
            'test_data_provider_1',
        )
        data_provider_2 = TestDataProvider.register_data_provider(
            'test_data_provider_2',
        )
        TestDataProvider.register_data_provider('test_data_provider_3')
        plot_1 = test_data.get_plot_data_1(data_provider_1)
        plot_2 = test_data.get_plot_data_2(data_provider_2)
        ins = niamoto_db_meta.plot.insert().values(plot_1 + plot_2)
        with Connector.get_connection() as connection:
            connection.execute(ins)
        fix_db_sequences()

    def test_get_current_plot_data(self):
        """
        :return: Test for get_current_plot_data_method.
            Test the structure of the returned DataFrame.
            Test retrieving an empty DataFrame.
            Test retrieving a not-empty DataFrame.
        """
        data_provider_1 = TestDataProvider('test_data_provider_1')
        data_provider_2 = TestDataProvider('test_data_provider_2')
        data_provider_3 = TestDataProvider('test_data_provider_3')
        with Connector.get_connection() as connection:
            pp1 = BasePlotProvider(data_provider_1)
            pp2 = BasePlotProvider(data_provider_2)
            pp3 = BasePlotProvider(data_provider_3)
            df1 = pp1.get_niamoto_plot_dataframe(connection)
            df2 = pp2.get_niamoto_plot_dataframe(connection)
            df3 = pp3.get_niamoto_plot_dataframe(connection)
            self.assertEqual(len(df1), 4)
            self.assertEqual(len(df2), 2)
            self.assertEqual(len(df3), 0)
            #  2. Check the structure of the DataFrame
            df_cols = list(df1.columns) + [df1.index.name, ]
            db_cols = niamoto_db_meta.plot.columns
            for db_col in db_cols:
                self.assertIn(db_col.name, df_cols)

    def test_get_insert_dataframe(self):
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            pp1 = BasePlotProvider(data_provider_1)
            df1 = pp1.get_niamoto_plot_dataframe(connection)
        #  1. Nothing to insert
        plot_1 = pd.DataFrame.from_records([
            {
                'id': 0,
                'name': 'plot_1_1',
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        ins = pp1.get_insert_dataframe(df1, plot_1)
        self.assertEqual(len(ins), 0)
        # 2. Everything to insert
        plot_2 = pd.DataFrame.from_records([
            {
                'id': 10,
                'name': 'plot_1_11',
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 11,
                'name': 'plot_1_12',
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        ins = pp1.get_insert_dataframe(df1, plot_2)
        self.assertIn('provider_pk', ins.columns)
        self.assertIn('provider_id', ins.columns)
        self.assertEqual(len(ins[pd.isnull(ins['provider_pk'])]), 0)
        self.assertEqual(len(ins[pd.isnull(ins['provider_id'])]), 0)
        self.assertEqual(len(ins), 2)
        # 3. Partial insert
        plot_3 = pd.DataFrame.from_records([
            {
                'id': 0,
                'name': 'plot_1_1',
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 11,
                'name': 'plot_1_12',
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        ins = pp1.get_insert_dataframe(df1, plot_3)
        self.assertEqual(len(ins), 1)

    def test_get_update_dataframe(self):
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            pp1 = BasePlotProvider(data_provider_1)
            df1 = pp1.get_niamoto_plot_dataframe(connection)
        #  1. Nothing to update
        plot_1 = pd.DataFrame.from_records([
            {
                'id': 10,
                'name': 'plot_1_11',
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        update_df = pp1.get_update_dataframe(df1, plot_1)
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
        self.assertEqual(len(update_df), 0)
        #  2. Everything to update
        plot_2 = pd.DataFrame.from_records([
            {
                'id': 0,
                'name': 'plot_1_a',
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 1,
                'name': 'plot_1_b',
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
            {
                'id': 2,
                'name': 'plot_1_c',
                'location': from_shape(Point(166.552, -22.097), srid=4326),
                'properties': '{}',
            },
            {
                'id': 5,
                'name': 'plot_1_d',
                'location': from_shape(Point(166.553, -22.099), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        update_df = pp1.get_update_dataframe(df1, plot_2)
        self.assertEqual(len(update_df), 4)
        #  3. Partial update
        plot_3 = pd.DataFrame.from_records([
            {
                'id': 0,
                'name': 'plot_1_z',
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 1,
                'name': 'plot_1_zzz',
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        update_df = pp1.get_update_dataframe(df1, plot_3)
        self.assertEqual(len(update_df), 2)

    def test_get_delete_dataframe(self):
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            pp1 = BasePlotProvider(data_provider_1)
            df1 = pp1.get_niamoto_plot_dataframe(connection)
        #  1. Nothing to delete
        plot_1 = pd.DataFrame.from_records([
            {
                'id': 0,
                'name': 'plot_1_1',
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
            {
                'id': 1,
                'name': 'plot_1_2',
                'location': from_shape(Point(166.551, -22.098), srid=4326),
                'properties': '{}',
            },
            {
                'id': 2,
                'location': from_shape(Point(166.552, -22.097), srid=4326),
                'properties': '{}',
            },
            {
                'id': 5,
                'location': from_shape(Point(166.553, -22.099), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        delete_df = pp1.get_delete_dataframe(df1, plot_1)
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
        plot_2 = pd.DataFrame.from_records([
            {
                'id': 10,
                'name': 'plot_1_11',
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        delete_df = pp1.get_delete_dataframe(df1, plot_2)
        self.assertEqual(len(delete_df), 4)
        self.assertEqual(len(delete_df[pd.isnull(delete_df['location'])]), 0)
        #  3. Partial delete
        plot_3 = pd.DataFrame.from_records([
            {
                'id': 0,
                'name': 'plot_1_1',
                'location': from_shape(Point(166.5521, -22.0939), srid=4326),
                'properties': '{}',
            },
        ], index='id')
        delete_df = pp1.get_delete_dataframe(df1, plot_3)
        self.assertEqual(len(delete_df), 3)
        self.assertEqual(len(delete_df[pd.isnull(delete_df['location'])]), 0)

    def test_sync_insert(self):
        self.tearDownClass()
        self.setUpClass()
        data_provider_3 = TestDataProvider('test_data_provider_3')
        with Connector.get_connection() as connection:
            pp3 = BasePlotProvider(data_provider_3)
            self.assertEqual(
                len(pp3.get_niamoto_plot_dataframe(connection)),
                0
            )
            pl = pd.DataFrame.from_records([
                {
                    'id': 0,
                    'name': 'plot_3_1',
                    'location': from_shape(
                        Point(166.5521, -22.0939),
                        srid=4326
                    ),
                    'properties': '{}',
                },
                {
                    'id': 1,
                    'name': 'plot_3_2',
                    'location': from_shape(
                        Point(166.551, -22.098),
                        srid=4326
                    ),
                    'properties': '{}',

                },
            ], index='id')
            i, u, d = pp3._sync(pl, connection)
            self.assertEqual(len(i), 2)
            self.assertEqual(len(u), 0)
            self.assertEqual(len(d), 0)
            self.assertEqual(
                len(pp3.get_niamoto_plot_dataframe(connection)),
                2
            )

    def test_sync_update(self):
        self.tearDownClass()
        self.setUpClass()
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            pp1 = BasePlotProvider(data_provider_1)
            self.assertEqual(
                len(pp1.get_niamoto_plot_dataframe(connection)),
                4
            )
            pl = pd.DataFrame.from_records([
                {
                    'id': 0,
                    'name': "plot_1",
                    'properties': None,
                    'location': WKTElement(
                        Point(166.5521, -22.0939).wkt,
                        srid=4326
                    ),
                    'properties': '{}',
                },
                {
                    'id': 1,
                    'name': 'plot_b',
                    'properties': None,
                    'location': from_shape(Point(166.551, -22.098), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 2,
                    'name': 'plot_c',
                    'properties': {'yo': 'yo'},
                    'location': from_shape(Point(166.552, -22.097), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 5,
                    'name': 'plot_d',
                    'properties': {},
                    'location': WKTElement(
                        Point(166.553, -22.099),
                        srid=4326
                    ),
                    'properties': '{}',
                },
            ], index='id')
            i, u, d = pp1._sync(pl, connection)
            self.assertEqual(len(i), 0)
            self.assertEqual(len(u), 4)
            self.assertEqual(len(d), 0)
            self.assertEqual(
                len(pp1.get_niamoto_plot_dataframe(connection)),
                4
            )

    def test_sync_delete(self):
        self.tearDownClass()
        self.setUpClass()
        data_provider_1 = TestDataProvider('test_data_provider_1')
        with Connector.get_connection() as connection:
            pp1 = BasePlotProvider(data_provider_1)
            self.assertEqual(
                len(pp1.get_niamoto_plot_dataframe(connection)),
                4
            )
            occ = pd.DataFrame.from_records(
                [],
                index='id',
                columns=('id', 'location')
            )
            i, u, d = pp1._sync(occ, connection)
            self.assertEqual(len(i), 0)
            self.assertEqual(len(u), 0)
            self.assertEqual(len(d), 4)
            self.assertEqual(
                len(pp1.get_niamoto_plot_dataframe(connection)),
                0
            )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

