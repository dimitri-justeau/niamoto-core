# coding: utf-8

import unittest
import os
import logging

from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import Table
import geopandas as gpd

from niamoto.testing import set_test_path

set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.vector.vector_manager import VectorManager
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector
from niamoto.exceptions import RecordAlreadyExistsError, NoRecordFoundError


SHP_TEST = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)


class TestVectorManager(BaseTestNiamotoSchemaCreated):
    """
    Test case for VectorManager class.
    """

    def tearDown(self):
        delete_stmt = meta.vector_registry.delete()
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_VECTOR_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE IF EXISTS {};".format(
                    "{}.{}".format(settings.NIAMOTO_VECTOR_SCHEMA, tb)
                ))
            connection.execute(delete_stmt)

    def test_get_vector_list(self):
        df1 = VectorManager.get_vector_list()
        self.assertEqual(len(df1), 0)

    def test_add_vector(self):
        #  Test non existing raster
        null_path = os.path.join(NIAMOTO_HOME, "NULL.shp")
        self.assertRaises(
            FileNotFoundError,
            VectorManager.add_vector,
            "null_vector",
            null_path,
        )
        VectorManager.add_vector("ncl_adm1", SHP_TEST)
        self.assertRaises(
            RecordAlreadyExistsError,
            VectorManager.add_vector,
            "ncl_adm1",
            SHP_TEST,
        )
        df = VectorManager.get_vector_list()
        self.assertEqual(len(df), 1)
        self.assertEqual(df['name'].iloc[0], 'ncl_adm1')
        engine = Connector.get_engine()
        inspector = Inspector.from_engine(engine)
        self.assertIn(
            'ncl_adm1',
            inspector.get_table_names(schema=settings.NIAMOTO_VECTOR_SCHEMA),
        )

    def test_update_vector(self):
        # Add raster
        VectorManager.add_vector('ncl_adm1', SHP_TEST)
        # Update raster
        VectorManager.update_vector('ncl_adm1', SHP_TEST, new_name='ncl')
        VectorManager.update_vector('ncl', SHP_TEST)
        self.assertRaises(
            NoRecordFoundError,
            VectorManager.update_vector,
            'ncl_adm1',
            SHP_TEST,
        )
        null_path = os.path.join(NIAMOTO_HOME, "NULL.shp")
        self.assertRaises(
            FileNotFoundError,
            VectorManager.add_vector,
            "ncl_bis",
            null_path,
        )
        VectorManager.add_vector('ncl_adm', SHP_TEST)
        self.assertRaises(
            RecordAlreadyExistsError,
            VectorManager.update_vector,
            'ncl',
            SHP_TEST,
            new_name='ncl_adm'
        )
        df = VectorManager.get_vector_list()
        self.assertIn('ncl', list(df['name']))
        engine = Connector.get_engine()
        inspector = Inspector.from_engine(engine)
        self.assertIn(
            'ncl',
            inspector.get_table_names(schema=settings.NIAMOTO_VECTOR_SCHEMA),
        )
        self.assertNotIn(
            'ncl_adm1',
            inspector.get_table_names(schema=settings.NIAMOTO_VECTOR_SCHEMA),
        )
        VectorManager.update_vector(
            'ncl',

        )

    def test_delete_vector(self):
        VectorManager.add_vector('ncl_adm1', SHP_TEST)
        VectorManager.delete_vector('ncl_adm1')
        df = VectorManager.get_vector_list()
        self.assertNotIn('ncl_adm1', list(df['name']))
        engine = Connector.get_engine()
        inspector = Inspector.from_engine(engine)
        self.assertNotIn(
            'ncl_adm1',
            inspector.get_table_names(schema=settings.NIAMOTO_VECTOR_SCHEMA),
        )
        self.assertRaises(
            NoRecordFoundError,
            VectorManager.delete_vector,
            'ncl_adm1'
        )

    def test_get_geometry_column(self):
        VectorManager.add_vector('ncl_adm1', SHP_TEST)
        geom_col = VectorManager.get_geometry_column('ncl_adm1')
        self.assertEqual(geom_col, ('wkb_geometry', 'MULTIPOLYGON', 4326))

    def test_get_vector_primary_key_columns(self):
        VectorManager.add_vector('ncl_adm1', SHP_TEST)
        pk_cols = VectorManager.get_vector_primary_key_columns('ncl_adm1')
        self.assertEqual(pk_cols, [('ogc_fid', 'integer')])

    def test_get_vector_geo_dataframe(self):
        VectorManager.add_vector('ncl_adm1', SHP_TEST)
        df = VectorManager.get_vector_geo_dataframe('ncl_adm1')
        self.assertIsInstance(df, gpd.GeoDataFrame)

    def test_get_vector_sqlalchemy_table(self):
        VectorManager.add_vector('ncl_adm1', SHP_TEST)
        table = VectorManager.get_vector_sqlalchemy_table('ncl_adm1')
        self.assertIsInstance(table, Table)
        self.assertEqual(table.name, 'ncl_adm1')


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
