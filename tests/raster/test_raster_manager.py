# coding: utf-8

import unittest
import os
from datetime import datetime
import logging

from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.raster.raster_manager import RasterManager
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector


class TestRasterManager(BaseTestNiamotoSchemaCreated):
    """
    Test case for RasterManager class.
    """

    def tearDown(self):
        delete_stmt = niamoto_db_meta.raster_registry.delete()
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_RASTER_SCHEMA
            )
            for tb in tables:
                if tb != niamoto_db_meta.raster_registry.name:
                    connection.execute("DROP TABLE IF EXISTS {};".format(
                        "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, tb)
                    ))
            connection.execute(delete_stmt)

    def test_get_raster_list(self):
        df1 = RasterManager.get_raster_list()
        self.assertEqual(len(df1), 0)
        data = [
            {
                'name': 'raster_1',
                'date_create': datetime.now(),
                'date_update': datetime.now(),
                'properties': {},
            },
            {
                'name': 'raster_2',
                'date_create': datetime.now(),
                'date_update': datetime.now(),
                'properties': {},
            },
            {
                'name': 'raster_3',
                'date_create': datetime.now(),
                'date_update': datetime.now(),
                'properties': {},
            },
        ]
        ins = niamoto_db_meta.raster_registry.insert().values(data)
        with Connector.get_connection() as connection:
            connection.execute(ins)
        df2 = RasterManager.get_raster_list()
        self.assertEqual(len(df2), 3)

    def test_add_raster(self):
        #  Test non existing raster
        null_path = os.path.join(NIAMOTO_HOME, "NULL.tif")
        self.assertRaises(
            FileNotFoundError,
            RasterManager.add_raster,
            "null_raster",
            null_path,
            tile_dimension=(200, 200)
        )
        # Test existing raster
        test_raster = os.path.join(
            NIAMOTO_HOME,
            "data",
            "raster",
            "rainfall_wgs84.tif"
        )
        RasterManager.add_raster(
            "rainfall",
            test_raster,
        )
        df = RasterManager.get_raster_list()
        self.assertEqual(len(df), 1)
        self.assertEqual(df['name'].iloc[0], 'rainfall')
        engine = Connector.get_engine()
        inspector = Inspector.from_engine(engine)
        self.assertIn(
            'rainfall',
            inspector.get_table_names(schema=settings.NIAMOTO_RASTER_SCHEMA),
        )

    def test_update_raster(self):
        # Add raster
        test_raster = os.path.join(
            NIAMOTO_HOME,
            "data",
            "raster",
            "rainfall_wgs84.tif"
        )
        RasterManager.add_raster(
            "rainfall",
            test_raster,
            tile_dimension=(200, 200),
        )
        # Update raster
        RasterManager.update_raster(
            "rainfall",
            test_raster,
            new_name="rainfall_new",
            tile_dimension=(100, 100),
        )
        df = RasterManager.get_raster_list()
        engine = Connector.get_engine()
        inspector = Inspector.from_engine(engine)
        self.assertIn(
            'rainfall_new',
            inspector.get_table_names(schema=settings.NIAMOTO_RASTER_SCHEMA),
        )
        self.assertNotIn(
            'rainfall',
            inspector.get_table_names(schema=settings.NIAMOTO_RASTER_SCHEMA),
        )
        # Update raster, only properties
        RasterManager.update_raster(
            "rainfall_new",
            properties={'test': 10}
        )

    def test_delete_raster(self):
        test_raster = os.path.join(
            NIAMOTO_HOME,
            "data",
            "raster",
            "rainfall_wgs84.tif"
        )
        RasterManager.add_raster(
            "rainfall",
            test_raster,
            tile_dimension=(200, 200),
        )
        RasterManager.delete_raster("rainfall")

    def test_raster_srid(self):
        test_raster = os.path.join(
            NIAMOTO_HOME,
            "data",
            "raster",
            "rainfall_wgs84.tif"
        )
        srid = RasterManager.get_raster_srid(test_raster)
        self.assertEqual(srid, 4326)
        df = RasterManager.get_raster_list()
        self.assertEqual(len(df), 0)

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
