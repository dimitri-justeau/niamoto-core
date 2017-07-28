# coding: utf-8

import os
import unittest

from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.api.vector_api import add_vector
from niamoto.api.data_marts_api import create_vector_dimension
from niamoto.data_marts.dimensions.vector_hierarchy_dimension import \
    VectorHierarchyDimension
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta


SHP_TEST_0 = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm0.shp'
)
SHP_TEST_1 = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)
SHP_TEST_2 = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm2.shp'
)


class TestVectorHierarchyDimension(BaseTestNiamotoSchemaCreated):
    """
    Test case for VectorHierarchyDimension class.
    """

    @classmethod
    def setUpClass(cls):
        super(TestVectorHierarchyDimension, cls).setUpClass()
        add_vector(SHP_TEST_1, 'ncl_adm0')
        add_vector(SHP_TEST_1, 'ncl_adm1')
        add_vector(SHP_TEST_2, 'ncl_adm2')

    def setUp(self):
        super(TestVectorHierarchyDimension, self).setUp()
        self.tearDown()

    @classmethod
    def tearDownClass(cls):
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_VECTOR_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE IF EXISTS {};".format(
                    "{}.{}".format(settings.NIAMOTO_VECTOR_SCHEMA, tb)
                ))
            delete_stmt = meta.vector_registry.delete()
            connection.execute(delete_stmt)

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
                connection.execute("DROP TABLE {};".format(
                    "{}.{}".format(settings.NIAMOTO_DIMENSIONS_SCHEMA, tb)
                ))
            delete_stmt = meta.dimension_registry.delete()
            connection.execute(delete_stmt)

    def test_vector_dimension(self):
        dim0 = create_vector_dimension('ncl_adm0')
        dim1 = create_vector_dimension('ncl_adm1')
        dim2 = create_vector_dimension('ncl_adm2')
        vh_dim = VectorHierarchyDimension('ncl_adm', [dim0, dim1, dim2])
        print(vh_dim.columns)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
