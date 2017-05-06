# coding: utf-8

import os
import unittest


if __name__ == "__main__":
    os.environ.setdefault(
        "NIAMOTO_SETTINGS_MODULE",
        "niamoto.testing.test_settings"
    )
    from niamoto.conf import settings
    from niamoto.testing.test_database_manager import TestDatabaseManager
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    test_suite = unittest.TestLoader().discover('tests')
    unittest.TextTestRunner(verbosity=1).run(test_suite)
    TestDatabaseManager.teardown_test_database()
