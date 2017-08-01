# coding: utf-8

import unittest

from sqlalchemy.exc import IntegrityError

from niamoto.testing import set_test_path
set_test_path()

from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.db.connector import Connector
from niamoto.db import metadata as niamoto_db_meta
from niamoto.conf import settings
from niamoto.exceptions import MalformedDataSourceError, NoRecordFoundError, \
    RecordAlreadyExistsError
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_data_provider import TestDataProvider


class TestSynonymsTaxon(BaseTestNiamotoSchemaCreated):
    """
    Test for synonyms taxon methods.
    """

    @classmethod
    def setUpClass(cls):
        super(TestSynonymsTaxon, cls).setUpClass()
        TestDataProvider.register_data_provider('test_data_provider_1')
        TestDataProvider.register_data_provider('test_data_provider_2')

    def tearDown(self):
        TaxonomyManager.delete_all_taxa()
        TaxonomyManager.unregister_all_synonym_keys()

    def test_get_synonym_keys(self):
        df = TaxonomyManager.get_synonym_keys()
        self.assertEqual(len(df), 0)

    def test_register_unregister_synonym_key(self):
        TaxonomyManager.register_synonym_key("synonym_key_1")
        self.assertRaises(
            RecordAlreadyExistsError,
            TaxonomyManager.register_synonym_key,
            "synonym_key_1"
        )
        df = TaxonomyManager.get_synonym_keys()
        self.assertEqual(len(df), 1)
        TaxonomyManager.unregister_synonym_key("synonym_key_1")
        self.assertRaises(
            NoRecordFoundError,
            TaxonomyManager.unregister_synonym_key,
            "synonym_key"
        )
        df = TaxonomyManager.get_synonym_keys()
        self.assertEqual(len(df), 0)
        # Test with bind
        with Connector.get_connection() as connection:
            TaxonomyManager.register_synonym_key("test", bind=connection)
            TaxonomyManager.assert_synonym_key_exists("test", bind=connection)
            TaxonomyManager.unregister_synonym_key("test", bind=connection)
            TaxonomyManager.assert_synonym_key_does_not_exists(
                "test",
                bind=connection
            )

    def test_add_single_synonym(self):
        synonym_key = "synonym_key_1"
        TaxonomyManager.register_synonym_key("synonym_key_1")
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
        ]
        ins = niamoto_db_meta.taxon.insert().values(data)
        with Connector.get_connection() as connection:
            connection.execute(ins)
        TaxonomyManager.add_synonym_for_single_taxon(0, synonym_key, 1)
        df1 = TaxonomyManager.get_raw_taxon_dataframe()
        self.assertEqual(
            df1.loc[0]['synonyms'],
            {synonym_key: 1}
        )
        TaxonomyManager.add_synonym_for_single_taxon(0, synonym_key, 2)
        df2 = TaxonomyManager.get_raw_taxon_dataframe()
        self.assertEqual(
            df2.loc[0]['synonyms'],
            {
                synonym_key: 1,
                synonym_key: 2,
            }
        )

    def test_duplicate_synonym(self):
        synonym_key = "synonym_key_1"
        TaxonomyManager.register_synonym_key("synonym_key_1")
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
                'full_name': 'Family Two',
                'rank_name': 'Two',
                'rank': niamoto_db_meta.TaxonRankEnum.FAMILIA,
                'parent_id': None,
                'synonyms': {},
                'mptt_left': 0,
                'mptt_right': 0,
                'mptt_tree_id': 0,
                'mptt_depth': 0,
            },
        ]
        ins = niamoto_db_meta.taxon.insert().values(data)
        with Connector.get_connection() as connection:
            connection.execute(ins)
        TaxonomyManager.add_synonym_for_single_taxon(0, synonym_key, 1)
        self.assertRaises(
            IntegrityError,
            TaxonomyManager.add_synonym_for_single_taxon,
            1, synonym_key, 1,
        )

    def test_get_synonyms_map(self):
        synonym_key = "synonym_key_1"
        TaxonomyManager.register_synonym_key("synonym_key_1")
        data = [
            {
                'id': 0,
                'full_name': 'Family One',
                'rank_name': 'One',
                'rank': niamoto_db_meta.TaxonRankEnum.FAMILIA,
                'parent_id': None,
                'synonyms': {
                    synonym_key: 10,
                },
                'mptt_left': 0,
                'mptt_right': 0,
                'mptt_tree_id': 0,
                'mptt_depth': 0,
            },
            {
                'id': 1,
                'full_name': 'Family Two',
                'rank_name': 'Two',
                'rank': niamoto_db_meta.TaxonRankEnum.FAMILIA,
                'parent_id': None,
                'synonyms': {
                    synonym_key: 20,
                },
                'mptt_left': 0,
                'mptt_right': 0,
                'mptt_tree_id': 0,
                'mptt_depth': 0,
            },
        ]
        ins = niamoto_db_meta.taxon.insert().values(data)
        with Connector.get_connection() as connection:
            connection.execute(ins)
        synonyms = TaxonomyManager.get_synonyms_for_key(synonym_key)
        self.assertEqual(synonyms.loc[10], 0)
        self.assertEqual(synonyms.loc[20], 1)

    def test_get_synonym_key(self):
        self.assertRaises(
            NoRecordFoundError,
            TaxonomyManager.get_synonym_key,
            'Not existing'
        )
        TaxonomyManager.register_synonym_key("test")
        TaxonomyManager.get_synonym_key("test")
        with Connector.get_connection() as connection:
            TaxonomyManager.get_synonym_key("test", bind=connection)

    def test_register_unregister_unique_constraints(self):
        TaxonomyManager._register_unique_synonym_key_constraint("Yo")
        TaxonomyManager._unregister_unique_synonym_key_constraint("Yo")

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

