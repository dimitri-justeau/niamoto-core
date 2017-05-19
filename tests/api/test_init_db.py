# coding: utf-8

import unittest

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.api.init_db import init_db


DB = settings.TEST_DATABASE


class TestInitDbApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for init_db api.
    """

    @classmethod
    def setUpClass(cls):
        super(TestInitDbApi, cls).setUpClass()

    def test_init_db(self):
        init_db(DB)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()