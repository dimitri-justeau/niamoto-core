# coding: utf-8

import unittest
import os

from click.testing import CliRunner

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.bin.commands.taxonomy import set_taxonomy_cli
from niamoto.testing.test_database_manager import TestDatabaseManager
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
            ["This is not a path", ],
        )
        self.assertEqual(result.exit_code, 1)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
