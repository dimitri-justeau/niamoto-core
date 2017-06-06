# coding: utf-8

import unittest
import os

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.api import taxonomy_api
from niamoto.exceptions import DataSourceNotFoundError


class TestTaxonomyApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for taxonomy api.
    """

    TAXONOMY_CSV_PATH = os.path.join(
        NIAMOTO_HOME,
        'data',
        'taxonomy',
        'taxonomy_1.csv',
    )

    @classmethod
    def setUpClass(cls):
        super(TestTaxonomyApi, cls).setUpClass()

    def test_set_taxonomy(self):
        nb, synonyms = taxonomy_api.set_taxonomy(self.TAXONOMY_CSV_PATH)
        self.assertEqual(nb, 4)
        self.assertEqual(synonyms, {'gbif', 'taxref'})
        self.assertRaises(
            DataSourceNotFoundError,
            taxonomy_api.set_taxonomy,
            "fake_csv_file_path"
        )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
