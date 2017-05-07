# coding: utf-8

import unittest

from niamoto.testing import set_test_path
set_test_path()


if __name__ == "__main__":
    from niamoto.conf import settings
    from niamoto.testing.test_database_manager import TestDatabaseManager
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    test_suite = unittest.TestLoader().discover('tests')
    unittest.TextTestRunner(verbosity=1).run(test_suite)
    TestDatabaseManager.teardown_test_database()
