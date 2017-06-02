# coding: utf-8

import os
import unittest

from click.testing import CliRunner
from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.bin.commands import raster
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TEST_RASTER = os.path.join(
    NIAMOTO_HOME,
    "data",
    "raster",
    "rainfall_wgs84.tif"
)


class TestCLIRaster(BaseTestNiamotoSchemaCreated):
    """
    Test case for raster cli methods.
    """

    def tearDown(self):
        delete_stmt = niamoto_db_meta.raster_registry.delete()
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_RASTER_SCHEMA
            )
            for tb in tables:
                if tb != niamoto_db_meta.raster_registry.name:
                    connection.execute("DROP TABLE IF EXISTS {};".format(
                        "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, tb)
                    ))
            connection.execute(delete_stmt)

    def test_list_rasters(self):
        runner = CliRunner()
        result = runner.invoke(raster.list_rasters_cli)
        self.assertEqual(result.exit_code, 0)

    def test_add_raster(self):
        runner = CliRunner()
        result = runner.invoke(raster.add_raster_cli, [
            'test_raster',
            '200', '200',
            TEST_RASTER,
        ], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(raster.add_raster_cli, [
            'test_raster',
            '200', '200',
            TEST_RASTER,
        ])
        self.assertEqual(result.exit_code, 1)

    def test_update_raster(self):
        runner = CliRunner()
        runner.invoke(raster.add_raster_cli, [
            'test_raster',
            '200', '200',
            TEST_RASTER
        ], catch_exceptions=False)
        result = runner.invoke(raster.update_raster_cli, [
            'test_raster',
            '100', '100',
            TEST_RASTER
        ], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(raster.update_raster_cli, [
            'yo',
            '100', '100',
            TEST_RASTER
        ])
        self.assertEqual(result.exit_code, 1)

    def test_delete_raster(self):
        runner = CliRunner()
        runner.invoke(raster.add_raster_cli, [
            'test_raster',
            '200', '200',
            TEST_RASTER
        ], catch_exceptions=False)
        result = runner.invoke(raster.delete_raster_cli, [
            'test_raster',
        ], catch_exceptions=False, input='N')
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(raster.delete_raster_cli, [
            '-y', True,
            'test_raster',
        ], catch_exceptions=False)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(raster.delete_raster_cli, [
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
