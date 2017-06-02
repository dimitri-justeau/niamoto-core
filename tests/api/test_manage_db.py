# coding: utf-8

import unittest

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings
from niamoto.testing.base_tests import BaseTest
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.api import manage_db


DB = settings.DEFAULT_DATABASE


class TestInitDbApi(BaseTest):
    """
    Test case for init_db_cli api.
    """

    @classmethod
    def setUpClass(cls):
        super(TestInitDbApi, cls).setUpClass()

    def test_init_db(self):
        manage_db.init_db()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
