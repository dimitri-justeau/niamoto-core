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
from niamoto.data_marts.dimensions.base_dimension import BaseDimension
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
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


class TestBaseDimension(BaseTestNiamotoSchemaCreated):
    """
    Test case for BaseDimension class.
    """

    def tearDown(self):
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE IF EXISTS {};".format(
                    "{}.{}".format(settings.NIAMOTO_DIMENSIONS_SCHEMA, tb)
                ))

    def test_base_dimension(self):
        dim_1 = BaseDimension(
            "test_dimension",
            columns=[sa.Column('value', sa.Integer)]
        )
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
        dim_2 = BaseDimension(
            "test_dimension",
            columns=[sa.Column('value', sa.Integer)]
        )
        self.assertEqual(dim_2._exists, True)

    def test_populate(self):
        dim = BaseDimension(
            "test_dimension",
            columns=[
                sa.Column('value', sa.Integer),
                sa.Column('category', sa.String)
            ]
        )
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
        dim = BaseDimension(
            "test_dimension",
            columns=[
                sa.Column('value', sa.Integer),
                sa.Column('category', sa.String)
            ],
            publisher=TestPublisher(),
        )
        dim.create_dimension()
        dim.populate_from_publisher()
        dim.get_values()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()