# coding: utf-8

import unittest

from tests.test_utils import TestDatabaseManager
from tests import BaseTestNiamotoSchemaCreated


class TestBaseOccurrenceProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for base occurrence provider.
    """
    pass


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
