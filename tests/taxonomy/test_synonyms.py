# coding: utf-8

import unittest

from sqlalchemy.exc import IntegrityError

from niamoto.taxonomy.taxon import Taxon
from niamoto.db.connector import Connector
from niamoto.db import metadata as niamoto_db_meta
from niamoto import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_data_provider import TestDataProvider


class TestSynonymsTaxon(BaseTestNiamotoSchemaCreated):
    """
    Test for synonyms taxon methods.
    """

    @classmethod
    def setUpClass(cls):
        super(TestSynonymsTaxon, cls).setUpClass()
        TestDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        TestDataProvider.register_data_provider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        TestDataProvider.register_data_provider(
            'test_data_provider_2',
            database=settings.TEST_DATABASE,
        )

    def tearDown(self):
        Taxon.delete_all_taxa(database=settings.TEST_DATABASE)

    def test_add_single_synonym(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
        data_provider_2 = TestDataProvider(
            'test_data_provider_2',
            database=settings.TEST_DATABASE,
        )
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
        with Connector.get_connection(settings.TEST_DATABASE) as connection:
            connection.execute(ins)
        Taxon.add_synonym_for_single_taxon(
            0,
            data_provider_1,
            1,
            database=settings.TEST_DATABASE,
        )
        df1 = Taxon.get_raw_taxon_dataframe(
            database=settings.TEST_DATABASE
        )
        self.assertEqual(
            df1.loc[0]['synonyms'],
            {data_provider_1.get_type_name(): 1}
        )
        Taxon.add_synonym_for_single_taxon(
            0,
            data_provider_2,
            2,
            database=settings.TEST_DATABASE
        )
        df2 = Taxon.get_raw_taxon_dataframe(
            database=settings.TEST_DATABASE
        )
        self.assertEqual(
            df2.loc[0]['synonyms'],
            {
                data_provider_1.get_type_name(): 1,
                data_provider_2.get_type_name(): 2,
            }
        )

    def test_duplicate_synonym(self):
        data_provider_1 = TestDataProvider(
            'test_data_provider_1',
            database=settings.TEST_DATABASE,
        )
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
        with Connector.get_connection(settings.TEST_DATABASE) as connection:
            connection.execute(ins)
        Taxon.add_synonym_for_single_taxon(
            0,
            data_provider_1,
            1,
            database=settings.TEST_DATABASE,
        )
        self.assertRaises(
            IntegrityError,
            Taxon.add_synonym_for_single_taxon,
            1, data_provider_1, 1,
            database=settings.TEST_DATABASE,
        )

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

