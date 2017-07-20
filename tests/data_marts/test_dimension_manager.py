# coding: utf-8

import unittest

from sqlalchemy.engine.reflection import Inspector
import sqlalchemy as sa
import pandas as pd

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_data_marts import TestDimension
from niamoto.data_marts.dimensions.dimension_manager import DimensionManager
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta


class TestDimensionManager(BaseTestNiamotoSchemaCreated):
    """
    Test case for DimensionManager class.
    """

    def setUp(self):
        super(TestDimensionManager, self).setUp()
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
                connection.execute("DROP TABLE {}.{}".format(
                    settings.NIAMOTO_DIMENSIONS_SCHEMA, tb
                ))
            delete_stmt = meta.dimension_registry.delete()
            connection.execute(delete_stmt)

    def test_get_dimension_types(self):
        dim_types = DimensionManager.get_dimension_types()
        self.assertIsInstance(dim_types, dict)

    def test_registered_dimensions(self):
        dim = TestDimension()
        dim.create_dimension()
        registered = DimensionManager.get_registered_dimensions()
        self.assertIn(dim.name, list(registered['name']))

    def test_delete_dimension(self):
        dim = TestDimension()
        dim.create_dimension()
        DimensionManager.delete_dimension(dim.name)
        registered = DimensionManager.get_registered_dimensions()
        self.assertNotIn(dim.name, list(registered['name']))


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
