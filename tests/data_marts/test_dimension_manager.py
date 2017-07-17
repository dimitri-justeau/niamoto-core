# coding: utf-8

import unittest

from sqlalchemy.engine.reflection import Inspector
import sqlalchemy as sa
import pandas as pd

from niamoto.testing import set_test_path

set_test_path()

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.data_marts.dimensions.base_dimension import BaseDimension
from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.data_marts.dimensions.dimension_manager import DimensionManager
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
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE {}.{}".format(
                    settings.NIAMOTO_DIMENSIONS_SCHEMA, tb
                ))

    def test_get_dimension_types(self):
        dim_types = DimensionManager.get_dimension_types()
        self.assertIsInstance(dim_types, dict)

    def test_registered_dimensions(self):
        dim = TestDimension()
        dim.create_dimension()
        registered = DimensionManager.get_registered_dimensions()
        self.assertIn(dim.name, list(registered['name']))


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
