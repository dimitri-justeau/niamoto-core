# coding: utf-8

import os
import unittest
import logging

from click.testing import CliRunner
from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.api import vector_api
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.bin.commands import vector
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TEST_VECTOR = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)


class TestCLIVector(BaseTestNiamotoSchemaCreated):
    """
    Test case for vector cli methods.
    """

    def tearDown(self):
        delete_stmt = niamoto_db_meta.vector_registry.delete()
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

    def test_list_vectors(self):
        runner = CliRunner()
        result = runner.invoke(vector.list_vectors_cli)
        self.assertEqual(result.exit_code, 0)
        vector_api.add_vector(
            'test_vector',
            TEST_VECTOR,
        )
        result = runner.invoke(vector.list_vectors_cli)
        self.assertEqual(result.exit_code, 0)

    def test_add_vector(self):
        runner = CliRunner()
        result = runner.invoke(vector.add_vector_cli, [
            'test_vector',
            TEST_VECTOR,
        ], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(vector.add_vector_cli, [
            'test_vector',
            TEST_VECTOR,
        ])
        self.assertEqual(result.exit_code, 1)

    def test_update_vector(self):
        runner = CliRunner()
        runner.invoke(vector.add_vector_cli, [
            'test_vector',
            TEST_VECTOR
        ], catch_exceptions=False)
        result = runner.invoke(vector.update_vector_cli, [
            'test_vector',
            TEST_VECTOR,
            '--new_name', 'test_vector_updated',
        ], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(vector.update_vector_cli, [
            'yo',
            TEST_VECTOR
        ])
        self.assertEqual(result.exit_code, 1)

    def test_delete_vector(self):
        runner = CliRunner()
        runner.invoke(vector.add_vector_cli, [
            'test_vector',
            TEST_VECTOR
        ], catch_exceptions=False)
        result = runner.invoke(vector.delete_vector_cli, [
            'test_vector',
        ], catch_exceptions=False, input='N')
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(vector.delete_vector_cli, [
            '-y', True,
            'test_vector',
        ], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(vector.delete_vector_cli, [
            '-y', True,
            'yo',
        ])
        self.assertEqual(result.exit_code, 1)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
