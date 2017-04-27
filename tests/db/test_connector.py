# coding: utf-8

import unittest

from sqlalchemy.engine import Connection

from niamoto.db.connector import Connector
from niamoto.settings import NIAMOTO_SCHEMA, TEST_DATABASE
from niamoto.testing import BaseTest
from niamoto.testing.test_database_manager import TestDatabaseManager


class TestConnector(BaseTest):
    """
    Test case for db connector.
    """

    def test_get_connection(self):
        with Connector.get_connection(database=TEST_DATABASE) as connection:
            self.assertIsInstance(connection, Connection)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(NIAMOTO_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
