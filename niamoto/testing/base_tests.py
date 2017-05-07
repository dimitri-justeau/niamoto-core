# coding: utf-8

import unittest

from niamoto.db.connector import Connector
from niamoto.db.creator import Creator
from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager


class BaseTest(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        Connector.dispose_engines()


class BaseTestNiamotoSchemaCreated(BaseTest):

    @classmethod
    def setUpClass(cls):
        engine = Connector.get_engine(
            database=settings.TEST_DATABASE,
        )
        Creator.create_niamoto_schema(engine)

    @classmethod
    def tearDownClass(cls):
        engine = Connector.get_engine(
            database=settings.TEST_DATABASE,
        )
        Creator.drop_niamoto_schema(engine)
        super(BaseTestNiamotoSchemaCreated, cls).tearDownClass()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()