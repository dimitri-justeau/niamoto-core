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
from niamoto.api import data_marts_api
from niamoto.data_marts.dimensions.vector_dimension import VectorDimension


SHP_TEST = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)


class TestDataMartsApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for data marts api.
    """

    @classmethod
    def setUpClass(cls):
        super(TestDataMartsApi, cls).setUpClass()
        add_vector(SHP_TEST, 'ncl_adm1')

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

    def test_create_vector_dimension(self):
        data_marts_api.create_vector_dimension("ncl_adm1")

    def test_get_dimension(self):
        data_marts_api.create_vector_dimension("ncl_adm1")
        dim = data_marts_api.get_dimension("ncl_adm1")
        self.assertIsInstance(dim, VectorDimension)

    def test_delete_dimension(self):
        data_marts_api.create_vector_dimension("ncl_adm1")
        data_marts_api.delete_dimension("ncl_adm1")


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
