# coding: utf-8

import unittest

from niamoto.db.connector import Connector
from niamoto.db.creator import Creator
from tests.test_utils import TestDatabaseManager


class TestCreator(unittest.TestCase):
    """
    Test case for db creator.
    """

    @classmethod
    def tearDownClass(cls):
        Connector.dispose_engines()

    def test_create_niamoto_schema(self):
        engine = Connector.get_engine(
            TestDatabaseManager.TEST_USER,
            TestDatabaseManager.TEST_PASSWORD,
            host=TestDatabaseManager.HOST,
            database=TestDatabaseManager.TEST_DATABASE,
        )
        Creator.create_niamoto_schema(engine)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
