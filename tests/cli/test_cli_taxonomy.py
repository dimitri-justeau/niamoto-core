# coding: utf-8

import unittest
import os

from click.testing import CliRunner

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.bin.commands.taxonomy import set_taxonomy_cli, \
    map_all_synonyms_cli
from niamoto.bin.commands.status import get_general_status_cli
from niamoto.data_providers.provider_types import PROVIDER_TYPES
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


class TestCLITaxonomy(BaseTestNiamotoSchemaCreated):
    """
    Test case for taxonomy cli methods.
    """

    TAXONOMY_CSV_PATH = os.path.join(
        NIAMOTO_HOME,
        'data',
        'taxonomy',
        'taxonomy_1.csv',
    )

    def test_set_taxonomy(self):
        runner = CliRunner()
        result = runner.invoke(
            set_taxonomy_cli,
            [self.TAXONOMY_CSV_PATH, ],
        )
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(
            set_taxonomy_cli,
            [self.TAXONOMY_CSV_PATH, '--no_mapping', 'true'],
        )
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(
            set_taxonomy_cli,
            ["This is not a path", ],
        )
        self.assertEqual(result.exit_code, 1)

    def test_map_all_synonyms(self):
        runner = CliRunner()
        result = runner.invoke(
            map_all_synonyms_cli,
            [],
        )
        self.assertEqual(result.exit_code, 0)
        TestDataProvider.register_data_provider_type()
        TestDataProvider.register_data_provider("test")
        PROVIDER_TYPES[TestDataProvider.get_type_name()] = TestDataProvider
        result = runner.invoke(
            map_all_synonyms_cli,
            [],
        )
        self.assertEqual(result.exit_code, 0)

    def test_status(self):
        runner = CliRunner()
        result = runner.invoke(
            get_general_status_cli,
            [],
        )
        self.assertEqual(result.exit_code, 0)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
