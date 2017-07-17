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
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.data_publishers.base_fact_table_publisher import \
    BaseFactTablePublisher
from niamoto.data_marts.dimensions.base_dimension import BaseDimension
from niamoto.data_marts.fact_tables.base_fact_table import BaseFactTable
from niamoto.db.connector import Connector


class TestPublisher(BaseDataPublisher):
    def _process(self, *args, **kwargs):
        df = pd.DataFrame([
            {'idx': 0, 'value': 1, 'category': 'cat1'},
            {'idx': 1, 'value': 2, 'category': 'cat2'},
            {'idx': 2, 'value': 3, 'category': 'cat2'},
            {'idx': 3, 'value': 4, 'category': 'cat1'},
            {'idx': 4, 'value': 5, 'category': None},
        ])
        df.set_index(['idx'], inplace=True)
        return df


class TestFactTablePublisher(BaseFactTablePublisher):
    def _process(self, *args, **kwargs):
        df = pd.DataFrame([
            {'dim_1_id': 0, 'dim_2_id': 1, 'value': 1},
            {'dim_1_id': 0, 'dim_2_id': 2, 'value': 2},
            {'dim_1_id': 0, 'dim_2_id': 3, 'value': 3},
            {'dim_1_id': 0, 'dim_2_id': 4, 'value': 4},
            {'dim_1_id': 1, 'dim_2_id': 2, 'value': 3},
            {'dim_1_id': 2, 'dim_2_id': 4, 'value': 2},
            {'dim_1_id': 3, 'dim_2_id': 0, 'value': 1},
            {'dim_1_id': 3, 'dim_2_id': 4, 'value': None},
            {'dim_1_id': 3, 'dim_2_id': 3, 'value': 5},
            {'dim_1_id': 3, 'dim_2_id': 2, 'value': 4},
            {'dim_1_id': 3, 'dim_2_id': 1, 'value': 6},
        ])
        return df


class TestDimension(BaseDimension):

    def __init__(self, name="test_dimension", publisher=TestPublisher()):
        super(TestDimension, self).__init__(
            name,
            [sa.Column('value', sa.Integer), sa.Column('category', sa.String)],
            publisher=publisher
        )

    @classmethod
    def get_description(cls):
        return 'Test dimension'

    @classmethod
    def get_key(cls):
        return 'TEST_DIMENSION'


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
        dim_1 = TestDimension("dim_1")
        dim_2 = TestDimension("dim_2")
        ft = BaseFactTable(
            "test_fact",
            dimensions=[dim_1, dim_2],
            measurement_columns=[
                sa.Column('value', sa.Float),
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

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
