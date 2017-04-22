# coding: utf-8

import unittest

from sqlalchemy.engine import Connection

from niamoto.db.connector import Connector
from niamoto.settings import NIAMOTO_SCHEMA
from tests.test_utils import TestDatabaseManager


class TestConnector(unittest.TestCase):
    """
    Test case for db connector.
    """

    @classmethod
    def tearDownClass(cls):
        Connector.dispose_engines()

    def test_get_connection(self):
        connection = Connector.get_connection(
            TestDatabaseManager.TEST_USER,
            TestDatabaseManager.TEST_PASSWORD,
            host=TestDatabaseManager.HOST,
            database=TestDatabaseManager.TEST_DATABASE,
            schema=NIAMOTO_SCHEMA,
        )
        self.assertIsInstance(connection, Connection)
        connection.close()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(NIAMOTO_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()