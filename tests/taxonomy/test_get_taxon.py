# coding: utf-8

import unittest

from niamoto.testing import set_test_path
set_test_path()

from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.db.connector import Connector
from niamoto.db import metadata as niamoto_db_meta
from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


class TestGetTaxon(BaseTestNiamotoSchemaCreated):
    """
    Test for get taxon methods.
    """

    @classmethod
    def setUpClass(cls):
        super(TestGetTaxon, cls).setUpClass()

    def tearDown(self):
        TaxonomyManager.delete_all_taxa()

    def test_get_empty_raw_taxon_dataset(self):
        df1 = TaxonomyManager.get_raw_taxon_dataframe()
        self.assertEqual(len(df1), 0)

    def test_get_not_empty_raw_taxon_dataset(self):
        data = [
            {
                'id': 0,
                'full_name': 'Family One',
                'rank_name': 'One',
                'rank': niamoto_db_meta.TaxonRankEnum.FAMILIA,
                'parent_id': None,
                'synonyms': {},
                'mptt_left': 0,
                'mptt_right': 0,
                'mptt_tree_id': 0,
                'mptt_depth': 0,
            },
            {
                'id': 1,
                'full_name': 'Genus Two',
                'rank_name': 'Two',
                'rank': niamoto_db_meta.TaxonRankEnum.GENUS,
                'parent_id': 0,
                'synonyms': {},
                'mptt_left': 0,
                'mptt_right': 0,
                'mptt_tree_id': 0,
                'mptt_depth': 0,
            },
            {
                'id': 2,
                'full_name': 'Species Three',
                'rank_name': 'Three',
                'rank': niamoto_db_meta.TaxonRankEnum.SPECIES,
                'parent_id': None,
                'synonyms': {},
                'mptt_left': 1,
                'mptt_right': 0,
                'mptt_tree_id': 0,
                'mptt_depth': 0,
            },
        ]
        ins = niamoto_db_meta.taxon.insert().values(data)
        with Connector.get_connection() as connection:
            connection.execute(ins)
        df1 = TaxonomyManager.get_raw_taxon_dataframe()
        self.assertEqual(len(df1), 3)

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

