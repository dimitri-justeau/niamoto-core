# coding: utf-8

import unittest

from sqlalchemy.engine import Connection

from tests import test_utils
from niamoto.db.connector import Connector


class TestConnector(unittest.TestCase):
    """
    Test case for db connector.
    """

    @classmethod
    def tearDownClass(cls):
        Connector.dispose_engines()

    def test_get_connection(self):
        connection = Connector.get_connection(
            test_utils.TEST_USER,
            test_utils.TEST_PASSWORD,
            host=test_utils.DEFAULT_HOST,  # TODO: Global test db conf
            database=test_utils.TEST_DATABASE,
            schema='niamoto',
        )
        self.assertIsInstance(connection, Connection)
        connection.close()


if __name__ == '__main__':
    test_db_manager = test_utils.TestDatabaseManager(interactive=True)
    test_db_manager.setup_test_database()
    test_db_manager.create_schema('niamoto')
    unittest.main(exit=False)
    test_db_manager.teardown_test_database()
