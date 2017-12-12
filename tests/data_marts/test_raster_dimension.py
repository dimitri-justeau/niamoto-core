# coding: utf-8

import os
import unittest

from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.api import raster_api
from niamoto.data_marts.dimensions.raster_dimension import RasterDimension
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta


TEST_RASTER = os.path.join(
    NIAMOTO_HOME,
    "data",
    "raster",
    "rainfall_wgs84.tif"
)


class TestRasterDimension(BaseTestNiamotoSchemaCreated):
    """
    Test case for RasterDimension class.
    """

    @classmethod
    def setUpClass(cls):
        super(TestRasterDimension, cls).setUpClass()
        raster_api.add_raster('rainfall', TEST_RASTER)

    def setUp(self):
        super(TestRasterDimension, self).setUp()
        self.tearDown()

    def tearDown(self):
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_FACT_TABLES_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE {};".format(
                    "{}.{}".format(settings.NIAMOTO_FACT_TABLES_SCHEMA, tb)
                ))
            delete_stmt = meta.fact_table_registry.delete()
            connection.execute(delete_stmt)
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE {};".format(
                    "{}.{}".format(settings.NIAMOTO_DIMENSIONS_SCHEMA, tb)
                ))
            delete_stmt = meta.dimension_registry.delete()
            connection.execute(delete_stmt)

    def test_raster_dimension(self):
        dim = RasterDimension('rainfall')
        dim.create_dimension()
        dim.populate_from_publisher()
        dim.get_values()

    def test_raster_dimension_with_cuts(self):
        cuts = (
            [1000, 3000],
            ["Low rainfall", "Medium rainfall", "High rainfall"]
        )
        dim = RasterDimension('rainfall', cuts=cuts)
        dim.create_dimension()
        dim.populate_from_publisher()
        df = dim.get_values()
        self.assertIn('category', df.columns)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
