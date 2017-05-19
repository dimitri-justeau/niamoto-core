# coding: utf-8

import unittest
import os

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings
from niamoto.api.data_providers import *
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider


class TestDataProvidersApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for data providers api.
    """

    @classmethod
    def setUpClass(cls):
        super(TestDataProvidersApi, cls).setUpClass()

    def test_get_data_provider_type_list(self):
        db = settings.TEST_DATABASE
        l1 = get_data_provider_type_list(database=db)
        self.assertEqual(len(l1), 0)
        PlantnoteDataProvider.register_data_provider_type(database=db)
        TestDataProvider.register_data_provider_type(database=db)
        l2 = get_data_provider_type_list(database=db)
        self.assertEqual(len(l2), 2)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
