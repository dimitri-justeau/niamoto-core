# coding: utf-8

import unittest

from niamoto.db.connector import Connector
from niamoto.db.creator import Creator
from niamoto.settings import TEST_DATABASE
from tests import BaseTest
from tests.test_utils import TestDatabaseManager


class TestCreator(BaseTest):
    """
    Test case for db creator.
    """

    def test_create_drop_niamoto_schema(self):
        engine = Connector.get_engine(
            database=TEST_DATABASE,
        )
        Creator.create_niamoto_schema(engine)
        Creator.drop_niamoto_schema(engine)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
