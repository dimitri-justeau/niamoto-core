# coding: utf-8

import unittest

from niamoto.settings import NIAMOTO_SCHEMA
from tests.test_utils import TestDatabaseManager


if __name__ == "__main__":
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(NIAMOTO_SCHEMA)
    test_suite = unittest.TestLoader().discover('.')
    unittest.TextTestRunner(verbosity=1).run(test_suite)
    # TestDatabaseManager.teardown_test_database()
