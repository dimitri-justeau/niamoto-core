# coding: utf-8

import unittest

import pandas as pd

from niamoto.taxonomy.taxon import Taxon
from niamoto import settings
from niamoto.db.connector import Connector
from niamoto.db import metadata as niamoto_db_meta
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing import BaseTestNiamotoSchemaCreated
from niamoto.testing.mptt import make_taxon_tree


class TestMPTT(BaseTestNiamotoSchemaCreated):
    """
    Test for mptt taxon methods.
    """

    @classmethod
    def setUpClass(cls):
        super(TestMPTT, cls).setUpClass()

    def tearDown(self):
        Taxon.delete_all_taxa(database=settings.TEST_DATABASE)

    def test_construct_mptt_simple_tree(self):
        tree = [
            [1, [1, ]],
            [1, ]
        ]
        data, last_id = make_taxon_tree(tree)
        ins = niamoto_db_meta.taxon.insert().values(data)
        with Connector.get_connection(settings.TEST_DATABASE) as connection:
            connection.execute(ins)
        Taxon.make_mptt(database=settings.TEST_DATABASE)
        mptt = Taxon.get_raw_taxon_dataframe(
            database=settings.TEST_DATABASE
        )
        self.assertEqual(list(mptt['mptt_tree_id']), [1, 1, 1, 1, 5, 5])
        self.assertEqual(list(mptt['mptt_left']), [1, 2, 4, 5, 1, 2])
        self.assertEqual(list(mptt['mptt_right']), [8, 3, 7, 6, 4, 3])
        self.assertEqual(list(mptt['mptt_depth']), [0, 1, 1, 2, 0, 1])
        self.assertEqual(
            [int(i) if pd.notnull(i) else None
             for i in mptt['parent_id'].tolist()],
            [None, 1, 1, 3, None, 5]
        )

    def test_construct_mptt_less_simple_tree(self):
        tree = [
            [1, [1, [1, ]]],
            [1, ],
            [1, ],
            [1, ],
            [1, [3, [1, [1, [1, ]]]]]
        ]
        data, last_id = make_taxon_tree(tree)
        self.assertEqual(len(data), 24)
        ins = niamoto_db_meta.taxon.insert().values(data)
        with Connector.get_connection(settings.TEST_DATABASE) as connection:
            connection.execute(ins)
        Taxon.make_mptt(database=settings.TEST_DATABASE)
        mptt = Taxon.get_raw_taxon_dataframe(
            database=settings.TEST_DATABASE
        )
        self.assertEqual(
            list(mptt['mptt_tree_id']),
            [1, 1, 1, 1, 1, 1, 7, 7, 9, 9, 11, 11, 13, 13, 13, 13, 13, 13, 13,
             13, 13, 13, 13, 13]
        )
        self.assertEqual(
            list(mptt['mptt_left']),
            [1, 2, 4, 5, 8, 9, 1, 2, 1, 2, 1, 2, 1, 2, 4, 5, 7, 9, 12, 13, 15,
             16, 19, 20]
        )
        self.assertEqual(
            list(mptt['mptt_right']),
            [12, 3, 7, 6, 11, 10, 4, 3, 4, 3, 4, 3, 24, 3, 11, 6, 8, 10, 23, 14,
             18, 17, 22, 21]
        )
        self.assertEqual(
            list(mptt['mptt_depth']),
            [0, 1, 1, 2, 1, 2, 0, 1, 0, 1, 0, 1, 0, 1, 1, 2, 2, 2, 1, 2, 2,
             3, 2, 3]
        )
        self.assertEqual(
            [int(i) if pd.notnull(i) else None
             for i in mptt['parent_id'].tolist()],
            [None, 1, 1, 3, 1, 5, None, 7, None, 9, None, 11, None, 13,
             13, 15, 15, 15, 13, 19, 19, 21, 19, 23]
        )

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
