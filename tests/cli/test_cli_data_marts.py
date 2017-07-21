# coding: utf-8

import unittest
import os

from click.testing import CliRunner

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.api import data_marts_api
from niamoto.api import vector_api
from niamoto.bin.commands import data_marts
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


SHP_TEST = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)


class TestCLIDataMarts(BaseTestNiamotoSchemaCreated):
    """
    Test case for data marts cli methods.
    """

    def test_list_dimension_types_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            data_marts.list_dimension_types_cli,
            []
        )
        self.assertEqual(result.exit_code, 0)

    def test_list_dimensions_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            data_marts.list_dimensions_cli,
            []
        )
        self.assertEqual(result.exit_code, 0)
        vector_api.add_vector(SHP_TEST, 'ncl_adm')
        data_marts_api.create_vector_dimension('ncl_adm')
        result = runner.invoke(
            data_marts.list_dimensions_cli,
            []
        )
        self.assertEqual(result.exit_code, 0)

    def test_list_fact_tables_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            data_marts.list_fact_tables_cli,
            []
        )
        self.assertEqual(result.exit_code, 0)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
