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
from niamoto.api import vector_api
from niamoto.exceptions import NoRecordFoundError


SHP_TEST = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)


class TestVectorApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for vector api.
    """

    @classmethod
    def setUpClass(cls):
        super(TestVectorApi, cls).setUpClass()

    def tearDown(self):
        delete_stmt = meta.vector_registry.delete()
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_VECTOR_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE IF EXISTS {};".format(
                    "{}.{}".format(settings.NIAMOTO_VECTOR_SCHEMA, tb)
                ))
            connection.execute(delete_stmt)

    def test_get_raster_list(self):
        df1 = vector_api.get_vector_list()
        self.assertEqual(len(df1), 0)

    def test_add_vector(self):
        vector_api.add_vector(
            'test_vector',
            SHP_TEST,
        )

    def test_update_vector(self):
        vector_api.add_vector(
            'test_vector',
            SHP_TEST,
        )
        vector_api.update_vector(
            'test_vector',
            SHP_TEST,
            new_name="new_test_vector"
        )

    def test_delete_raster(self):
        vector_api.add_vector(
            'test_vector',
            SHP_TEST,
        )
        vector_api.delete_vector('test_vector')
        self.assertRaises(
            NoRecordFoundError,
            vector_api.delete_vector,
            'test_vector',
        )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
