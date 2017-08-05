# coding: utf-8

import unittest
import os

from click.testing import CliRunner
from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.api import data_marts_api
from niamoto.api import vector_api
from niamoto.api import raster_api
from niamoto.bin.commands import data_marts
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.data_publishers.base_fact_table_publisher import \
    BaseFactTablePublisher
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta


SHP_TEST = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)

TEST_RASTER = os.path.join(
    NIAMOTO_HOME,
    "data",
    "raster",
    "rainfall_wgs84.tif"
)


class TestCLIDataMarts(BaseTestNiamotoSchemaCreated):
    """
    Test case for data marts cli methods.
    """

    @classmethod
    def setUpClass(cls):
        super(TestCLIDataMarts, cls).setUpClass()
        vector_api.add_vector(SHP_TEST, 'ncl_adm')
        raster_api.add_raster(TEST_RASTER, 'rainfall')

    def tearDown(self):
        super(TestCLIDataMarts, self).tearDown()
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

    def test_create_vector_dimension_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            data_marts.create_vector_dim_cli,
            ['ncl_adm', '--populate']
        )
        self.assertEqual(result.exit_code, 0)

    def test_create_raster_dimension_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            data_marts.create_raster_dim_cli,
            ['rainfall', '--populate']
        )
        self.assertEqual(result.exit_code, 0)

    def test_create_taxon_dimension_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            data_marts.create_taxon_dim_cli,
            ['--populate']
        )
        self.assertEqual(result.exit_code, 0)

    def test_create_occurrence_location_dimension_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            data_marts.create_occurrence_location_dim_cli,
            ['--populate']
        )
        self.assertEqual(result.exit_code, 0)

    def test_delete_dimension_cli(self):
        runner = CliRunner()
        runner.invoke(
            data_marts.create_vector_dim_cli,
            ['ncl_adm', '--populate']
        )
        result = runner.invoke(
            data_marts.delete_dimension_cli,
            ['ncl_adm', ]
        )
        self.assertEqual(result.exit_code, 0)

    def test_create_fact_table_cli(self):
        data_marts_api.create_vector_dimension('ncl_adm')
        runner = CliRunner()
        result = runner.invoke(
            data_marts.create_fact_table_cli,
            ['fact_table', '-d', 'ncl_adm', '-m', 'measure']
        )
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(
            data_marts.create_fact_table_cli,
            ['test', '-d', 'non', '-m', 'a']
        )
        self.assertEqual(result.exit_code, 1)

    def test_delete_fact_table_cli(self):
        data_marts_api.create_vector_dimension('ncl_adm')
        runner = CliRunner()
        runner.invoke(
            data_marts.create_fact_table_cli,
            ['fact_table', '-d', 'ncl_adm', '-m', 'measure']
        )
        result = runner.invoke(
            data_marts.delete_fact_table_cli,
            ['fact_table', ]
        )
        self.assertEqual(result.exit_code, 0)

    def test_populate_fact_table_cli(self):
        data_marts_api.create_taxon_dimension('taxon_dim')
        data_marts_api.create_fact_table('fact_table', ['taxon_dim'], ['n'])

        class TestCLIFactTablePublisher(BaseFactTablePublisher):
            @classmethod
            def get_key(cls):
                return 'test_cli_fact_table_publisher'

            def _process(self, *args, **kwargs):
                dim = data_marts_api.get_dimension('taxon_dim')
                df = dim.get_values()
                df['n'] = df.index
                df['taxon_dim_id'] = df.index
                return df[['taxon_dim_id', 'n']]

        runner = CliRunner()
        result = runner.invoke(
            data_marts.populate_fact_table_cli,
            ['fact_table', 'test_cli_fact_table_publisher']
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
