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
from niamoto.testing.test_data_marts import TestDimension, \
    TestFactTablePublisher
from niamoto.data_marts.fact_tables.base_fact_table import BaseFactTable
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta


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
            connection.execute(meta.fact_table_registry.delete())
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE {};".format(
                    "{}.{}".format(settings.NIAMOTO_DIMENSIONS_SCHEMA, tb)
                ))
            connection.execute(meta.dimension_registry.delete())

    def test_base_fact_table(self):
        dim_1 = TestDimension("dim_1")
        dim_2 = TestDimension("dim_2")
        ft = BaseFactTable(
            "test_fact",
            dimensions=[dim_1, dim_2],
            measurement_columns=[
                sa.Column('measure_1', sa.Float),
            ],
            publisher_cls=TestFactTablePublisher
        )
        dim_1.create_dimension()
        dim_2.create_dimension()
        dim_1.populate_from_publisher()
        dim_2.populate_from_publisher()
        self.assertFalse(ft.is_created())
        ft.create_fact_table()
        self.assertTrue(ft.is_created())
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_FACT_TABLES_SCHEMA
            )
            self.assertIn("test_fact", tables)
        ft.populate_from_publisher()

    def test_load(self):
        dim_1 = TestDimension("dim_1")
        dim_2 = TestDimension("dim_2")
        ft = BaseFactTable(
            "test_fact",
            dimensions=[dim_1, dim_2],
            measurement_columns=[
                sa.Column('measure_1', sa.Float),
            ],
            publisher_cls=TestFactTablePublisher
        )
        dim_1.create_dimension()
        dim_2.create_dimension()
        ft.create_fact_table()
        ft_bis = BaseFactTable.load('test_fact')
        self.assertEqual(
            [dim.name for dim in ft_bis.dimensions],
            ['dim_1', 'dim_2'],
        )
        self.assertEqual(
            [measure.name for measure in ft_bis.measurement_columns],
            ['measure_1', ]
        )
        self.assertEqual(ft_bis.name, ft.name)

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
