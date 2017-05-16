# coding: utf-8

import unittest

from niamoto.testing import set_test_path
set_test_path()

from niamoto.taxonomy.populate import *
from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider


class TestPopulateTaxon(BaseTestNiamotoSchemaCreated):
    """
    Test for populate taxon methods.
    """

    @classmethod
    def setUpClass(cls):
        super(TestPopulateTaxon, cls).setUpClass()

    def tearDown(self):
        Taxon.delete_all_taxa(database=settings.TEST_DATABASE)

    def test_load_ncpippn_taxon_dataframe_from_json(self):
        df = load_ncpippn_taxon_dataframe_from_json()
        self.assertTrue(len(df) > 0)

    def test_populate_ncpippn_taxon_database(self):
        df = load_ncpippn_taxon_dataframe_from_json()
        populate_ncpippn_taxon_database(
            df,
            database=settings.TEST_DATABASE,
        )
        taxa = Taxon.get_raw_taxon_dataframe(database=settings.TEST_DATABASE)
        self.assertTrue(len(taxa) > 0)
        test_taxa = taxa.iloc[20]
        synonyms = test_taxa["synonyms"]
        self.assertIn("taxref", synonyms.keys())
        self.assertIn(PlantnoteDataProvider.get_type_name(), synonyms.keys())
        self.assertEqual(
            synonyms[PlantnoteDataProvider.get_type_name()],
            test_taxa.name
        )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
