# coding: utf-8

import unittest
import os

from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.data_marts.dimensional_model import DimensionalModel, \
    load_model_from_dict
from niamoto.testing.test_data_marts import TestDimension
from niamoto.api.vector_api import add_vector, delete_vector
from niamoto.api.data_marts_api import delete_dimension, delete_fact_table
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta


SHP_TEST = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)


class TestDimensionalModel(BaseTestNiamotoSchemaCreated):
    """
    Test case for DimensionalModel class.
    """

    def setUp(self):
        super(TestDimensionalModel, self).setUp()
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

    def test_load_model_from_dict(self):
        model_dict = {
            'dimensions': [
                {
                    'name': 'dim_1',
                    'dimension_type': 'TEST_DIMENSION'
                },
                {
                    'name': 'dim_2',
                    'dimension_type': 'TEST_DIMENSION'
                }
            ],
            'fact_tables': [
                {
                    'name': 'fact_table_1',
                    'dimensions': ['dim_1', 'dim_2'],
                    'measures': ['measure_1'],
                    'publisher_key': 'TEST_FACT_TABLE_PUBLISHER',
                    "aggregates": [
                        {
                            "name": "measure_sum",
                            "function": "sum",
                            "measure": "measure_1"
                        },
                        {
                            "name": "record_count",
                            "function": "count"
                        }
                    ],
                }
            ]
        }
        model = load_model_from_dict(model_dict)
        self.assertIsInstance(model, DimensionalModel)
        self.assertIn('fact_table_1', model.fact_tables)
        self.assertIn('dim_1', model.dimensions)
        self.assertIn('dim_2', model.dimensions)

    def test_generate_cubes_model(self):
        model_dict = {
            'dimensions': [
                {
                    'name': 'dim_1',
                    'dimension_type': 'TEST_DIMENSION'
                },
                {
                    'name': 'dim_2',
                    'dimension_type': 'TEST_DIMENSION'
                }
            ],
            'fact_tables': [
                {
                    'name': 'fact_table_1',
                    'dimensions': ['dim_1', 'dim_2'],
                    'measures': ['measure_1'],
                    'publisher_key': 'TEST_FACT_TABLE_PUBLISHER',
                    "aggregates": [
                        {
                            "name": "measure_sum",
                            "function": "sum",
                            "measure": "measure_1"
                        },
                        {
                            "name": "record_count",
                            "function": "count"
                        }
                    ],
                }
            ]
        }
        model = load_model_from_dict(model_dict)
        model.create_model()
        model.populate_dimensions()
        model.populate_fact_tables()
        cubes_model = model.generate_cubes_model()
        self.assertIsInstance(cubes_model, dict)
        workspace = model.get_cubes_workspace()
        browser = workspace.browser('fact_table_1')
        browser.aggregate(drilldown=['dim_1'])

    def test_occurrence_observed_model(self):
        add_vector(SHP_TEST, 'ncl_adm1')
        model_dict = {
            'dimensions': [
                {
                    'name': 'ncl_adm1',
                    'label_col': 'NAME_1',
                    'dimension_type': 'VECTOR_DIMENSION'
                },
            ],
            'fact_tables': [
                {
                    'name': 'occurrence_in_polygon',
                    'dimensions': ['ncl_adm1', ],
                    'measures': ['nb_occurrence'],
                    "aggregates": [
                        {
                            "name": "occurrence_sum",
                            "function": "sum",
                            "measure": "nb_occurrence"
                        },
                        {
                            "name": "record_count",
                            "function": "count"
                        }
                    ],
                }
            ]
        }
        model = load_model_from_dict(model_dict)
        model.create_model()
        model.populate_dimensions()
        delete_fact_table('occurrence_in_polygon')
        delete_dimension('ncl_adm1')
        delete_vector('ncl_adm1')


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
