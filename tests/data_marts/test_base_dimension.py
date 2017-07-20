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
from niamoto.db.connector import Connector
from niamoto.db.metadata import dimension_registry


class TestBaseDimension(BaseTestNiamotoSchemaCreated):
    """
    Test case for BaseDimension class.
    """

    def setUp(self):
        super(TestBaseDimension, self).setUp()
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
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE {}.{}".format(
                    settings.NIAMOTO_DIMENSIONS_SCHEMA, tb
                ))
        with Connector.get_connection() as connection:
            connection.execute(dimension_registry.delete())

    def test_base_dimension(self):
        dim_1 = TestDimension()
        self.assertEqual(dim_1._exists, False)
        self.assertFalse(dim_1.is_created())
        dim_1.create_dimension()
        self.assertTrue(dim_1.is_created())
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
            )
            self.assertIn("test_dimension", tables)
        dim_2 = TestDimension()
        self.assertEqual(dim_2._exists, True)

    def test_populate(self):
        dim = TestDimension()
        dim.create_dimension()
        df = pd.DataFrame([
            {'value': 1, 'category': 'cat1'},
            {'value': 2, 'category': 'cat2'},
            {'value': 3, 'category': 'cat2'},
            {'value': 4, 'category': 'cat1'},
            {'value': 5, 'category': 'NS'},
        ])
        df.index.names = [dim.PK_COLUMN_NAME]
        dim.populate(df)
        df2 = dim.get_values()
        self.assertTrue((df['value'] == df2['value']).all())
        self.assertTrue((df['category'] == df2['category']).all())
        self.assertTrue((df.index == df2.index).all())

    def test_populate_from_publisher(self):
        dim = TestDimension()
        dim.create_dimension()
        dim.populate_from_publisher()
        dim.get_values()

    def test_drop_dimension(self):
        dim = TestDimension()
        dim.create_dimension()
        dim.drop_dimension()
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
            )
            self.assertNotIn(dim.name, tables)
            sel = sa.select([dimension_registry.c.name, ])
            registered = connection.execute(sel).fetchall()
            self.assertEqual(len(registered), 0)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
