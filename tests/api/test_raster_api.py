# coding: utf-8

import unittest
import os

from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.api import raster_api
from niamoto.exceptions import NoRecordFoundError


DB = settings.TEST_DATABASE


class TestRasterApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for raster api.
    """

    TEST_RASTER_PATH = os.path.join(
            NIAMOTO_HOME,
            "data",
            "raster",
            "rainfall_wgs84.tif"
        )

    @classmethod
    def setUpClass(cls):
        super(TestRasterApi, cls).setUpClass()

    def tearDown(self):
        delete_stmt = niamoto_db_meta.raster_registry.delete()
        with Connector.get_connection(database=DB) as connection:
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
        df1 = raster_api.get_raster_list(database=DB)
        self.assertEqual(len(df1), 0)

    def test_add_raster(self):
        raster_api.add_raster(
            self.TEST_RASTER_PATH,
            'test_raster',
            200, 200,
            database=DB
        )

    def test_update_raster(self):
        raster_api.add_raster(
            self.TEST_RASTER_PATH,
            'test_raster',
            200, 200,
            database=DB
        )
        raster_api.update_raster(
            self.TEST_RASTER_PATH,
            'test_raster',
            150, 150,
            database=DB
        )

    def test_delete_raster(self):
        raster_api.add_raster(
            self.TEST_RASTER_PATH,
            'test_raster',
            200, 200,
            database=DB
        )
        raster_api.delete_raster('test_raster', database=DB)
        self.assertRaises(
            NoRecordFoundError,
            raster_api.delete_raster,
            'test_raster',
            database=DB
        )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
