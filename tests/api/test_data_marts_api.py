# coding: utf-8

import unittest
import os
import logging

from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector
from niamoto.api.vector_api import add_vector
from niamoto.api.raster_api import add_raster
from niamoto.api import data_marts_api
from niamoto.testing.test_data_marts import TestDimension, \
    TestFactTablePublisher
from niamoto.data_marts.dimensions.vector_dimension import VectorDimension
from niamoto.data_marts.dimensional_model import DimensionalModel
from niamoto.exceptions import DimensionNotRegisteredError


SHP_TEST = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)
SHP_TEST_0 = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm0.shp'
)

TEST_RASTER = os.path.join(
    NIAMOTO_HOME,
    "data",
    "raster",
    "rainfall_wgs84.tif"
)


class TestDataMartsApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for data marts api.
    """

    @classmethod
    def setUpClass(cls):
        super(TestDataMartsApi, cls).setUpClass()
        add_vector(SHP_TEST, 'ncl_adm1')
        add_vector(SHP_TEST_0, 'ncl_adm0')
        add_raster(TEST_RASTER, 'rainfall')

    def tearDown(self):
        super(TestDataMartsApi, self).tearDown()
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_FACT_TABLES_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE {} CASCADE;".format(
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
                connection.execute("DROP TABLE {} CASCADE;".format(
                    "{}.{}".format(settings.NIAMOTO_DIMENSIONS_SCHEMA, tb)
                ))
            delete_stmt = meta.dimension_registry.delete()
            connection.execute(delete_stmt)

    def test_create_vector_dimension(self):
        data_marts_api.create_vector_dimension("ncl_adm1")

    def test_create_raster_dimension(self):
        data_marts_api.create_raster_dimension("rainfall")

    def test_create_vector_hierarchy_dimension(self):
        data_marts_api.create_vector_dimension("ncl_adm1")
        data_marts_api.create_vector_dimension("ncl_adm0")
        data_marts_api.create_vector_hierarchy_dimension(
            'vector_hierarchy',
            ["ncl_adm1", "ncl_adm0"],
            populate=False,
        )

    def test_create_taxon_dimension(self):
        data_marts_api.create_taxon_dimension()

    def test_create_occurrence_location_dimension(self):
        data_marts_api.create_occurrence_location_dimension()

    def test_get_dimension(self):
        data_marts_api.create_vector_dimension("ncl_adm1")
        dim = data_marts_api.get_dimension("ncl_adm1")
        self.assertIsInstance(dim, VectorDimension)

    def test_delete_dimension(self):
        data_marts_api.create_vector_dimension("ncl_adm1")
        data_marts_api.delete_dimension("ncl_adm1")

    def test_get_dimension_types(self):
        data_marts_api.get_dimension_types()

    def test_get_registered_fact_tables(self):
        fact_tables = data_marts_api.get_registered_fact_tables()
        self.assertEqual(len(fact_tables), 0)
        data_marts_api.create_vector_dimension("ncl_adm1")
        data_marts_api.create_fact_table(
            'fact_table',
            ['ncl_adm1', ],
            ['measure_1', ]
        )
        fact_tables = data_marts_api.get_registered_fact_tables()
        self.assertEqual(len(fact_tables), 1)

    def test_create_fact_table(self):
        data_marts_api.create_vector_dimension("ncl_adm1")
        data_marts_api.create_fact_table(
            'fact_table',
            ['ncl_adm1', ],
            ['measure_1', ],
        )
        data_marts_api.get_fact_table('fact_table')
        self.assertRaises(
            DimensionNotRegisteredError,
            data_marts_api.create_fact_table,
            'fact_table_2',
            ['ncl', ],
            ['measure_2', ],
        )

    def test_populate_fact_table(self):
        dim_1 = TestDimension("dim_1")
        dim_2 = TestDimension("dim_2")
        dim_1.create_dimension()
        dim_2.create_dimension()
        dim_1.populate_from_publisher()
        dim_2.populate_from_publisher()
        data_marts_api.create_fact_table(
            "test_fact",
            dimension_names=['dim_1', 'dim_2'],
            measure_names=['measure_1'],
        )
        data_marts_api.populate_fact_table(
            'test_fact',
            TestFactTablePublisher.get_key()
        )

    def test_get_dimensional_model(self):
        dim_1 = TestDimension("dim_1")
        dim_2 = TestDimension("dim_2")
        dim_1.create_dimension()
        dim_2.create_dimension()
        data_marts_api.create_fact_table(
            "test_fact",
            dimension_names=['dim_1', 'dim_2'],
            measure_names=['measure_1'],
        )
        dm = data_marts_api.get_dimensional_model(
            'test_fact',
            [
                {
                    "name": "measure_1_sum",
                    "function": "sum",
                    "measure": "measure_1",
                },
            ]
        )
        self.assertIsInstance(dm, DimensionalModel)
        wk = dm.get_cubes_workspace()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
