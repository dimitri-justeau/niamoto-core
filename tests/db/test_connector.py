# coding: utf-8

import unittest

from tests import test_utils
from niamoto.db.connector import Connector


class TestConnector(unittest.TestCase):
    """
    Test case for db connector.
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    test_db_manager = test_utils.TestDatabaseManager(interactive=True)
    test_db_manager.setup_test_database()
    unittest.main(exit=False)
    test_db_manager.teardown_test_database()
