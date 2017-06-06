# coding: utf-8

import unittest

import pandas as pd

from niamoto.testing import set_test_path
set_test_path()

from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.db import metadata as niamoto_db_meta
from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


class TestSetTaxonomy(BaseTestNiamotoSchemaCreated):

    @classmethod
    def setUpClass(cls):
        super(TestSetTaxonomy, cls).setUpClass()

    def tearDown(self):
        TaxonomyManager.delete_all_taxa()

    def test_set_taxonomy(self):
        data = pd.DataFrame.from_records([
            {
                'id': 0,
                'full_name': 'Family One',
                'rank_name': 'One',
                'rank': niamoto_db_meta.TaxonRankEnum.FAMILIA,
                'parent_id': None,
                'gbif': 5,
                'taxref': 1,
            },
            {
                'id': 1,
                'full_name': 'Genus Two',
                'rank_name': 'Two',
                'rank': niamoto_db_meta.TaxonRankEnum.GENUS,
                'parent_id': 0,
                'gbif': 10,
                'taxref': 2,
            },
            {
                'id': 2,
                'full_name': 'Species Three',
                'rank_name': 'Three',
                'rank': niamoto_db_meta.TaxonRankEnum.SPECIES,
                'parent_id': None,
                'gbif': 7,
                'taxref': 3,
            },
        ], index='id')
        result = TaxonomyManager.set_taxonomy(data)
        self.assertEqual(result.rowcount, 3)
        df = TaxonomyManager.get_raw_taxon_dataframe()
        self.assertEqual(len(df), 3)
        synonym_keys = TaxonomyManager.get_synonym_keys()
        self.assertEqual(len(synonym_keys), 2)
        identity_synonyms = TaxonomyManager.get_synonyms_for_key(
            TaxonomyManager.IDENTITY_SYNONYM
        )
        self.assertEqual(len(identity_synonyms), 3)
        null_synonyms = TaxonomyManager.get_synonyms_for_key(None)
        self.assertEqual(len(null_synonyms), 0)
        gbif_synonyms = TaxonomyManager.get_synonyms_for_key("gbif")
        self.assertEqual(len(gbif_synonyms), 3)
        taxref_synonyms = TaxonomyManager.get_synonyms_for_key('taxref')
        self.assertEqual(len(taxref_synonyms), 3)

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

