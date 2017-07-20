# coding: utf-8

import unittest

from sqlalchemy.engine.reflection import Inspector
import sqlalchemy as sa

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.data_marts.fact_tables.fact_table_manager import FactTableManager
from niamoto.testing.test_data_marts import TestDimension
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta
from niamoto.exceptions import FactTableNotRegisteredError


class TestFactTableManager(BaseTestNiamotoSchemaCreated):
    """
    Test case for FactTable class.
    """

    def setUp(self):
        super(TestFactTableManager, self).setUp()
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

    def test_fact_table_manager(self):
        dim_1 = TestDimension("dim_1")
        dim_2 = TestDimension("dim_2")
        dim_1.create_dimension()
        dim_2.create_dimension()
        self.assertRaises(
            FactTableNotRegisteredError,
            FactTableManager.assert_fact_table_is_registered,
            'test_fact',
        )
        FactTableManager.register_fact_table(
            "test_fact",
            dimensions=[dim_1, dim_2],
            measure_columns=[
                sa.Column('measure_1', sa.Float),
            ],
            publisher_cls=None
        )
        self.assertIn(
            'test_fact',
            list(FactTableManager.get_registered_fact_tables()['name'])
        )
        FactTableManager.assert_fact_table_is_registered('test_fact')
        ft = FactTableManager.get_fact_table('test_fact')
        self.assertEqual(ft.name, 'test_fact')
        FactTableManager.delete_fact_table('test_fact')
        self.assertFalse(ft.is_created())
        self.assertEqual(
            len(FactTableManager.get_registered_fact_tables()),
            0,
        )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
