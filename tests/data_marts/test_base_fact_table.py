# coding: utf-8

import unittest

from sqlalchemy.engine.reflection import Inspector
import sqlalchemy as sa

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.data_marts.dimensions.base_dimension import BaseDimension
from niamoto.data_marts.fact_tables.base_fact_table import BaseFactTable
from niamoto.db.connector import Connector


class TestBaseFactTable(BaseTestNiamotoSchemaCreated):
    """
    Test case for BaseFactTable class.
    """

    def setUp(self):
        super(TestBaseFactTable, self).setUp()
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
                connection.execute("DROP TABLE {};".format(
                    "{}.{}".format(settings.NIAMOTO_DIMENSIONS_SCHEMA, tb)
                ))

    def test_base_fact_table(self):
        dim_1 = BaseDimension(
            "test_dim_1",
            columns=[sa.Column('value', sa.Integer)]
        )
        dim_2 = BaseDimension(
            "test_dim_2",
            columns=[sa.Column('value', sa.Integer)]
        )
        ft = BaseFactTable(
            "test_fact",
            dimensions=[dim_1, dim_2],
            measurement_columns=[
                sa.Column('value', sa.Integer),
            ]
        )
        dim_1.create_dimension()
        dim_2.create_dimension()
        self.assertFalse(ft.is_created())
        ft.create_fact_table()
        self.assertTrue(ft.is_created())
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_FACT_TABLES_SCHEMA
            )
            self.assertIn("test_fact", tables)

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
