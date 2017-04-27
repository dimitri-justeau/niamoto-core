# coding: utf-8

import unittest

from niamoto.settings import TEST_DATABASE
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_data_provider import TestDataProvider


class TestBaseDataProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for base data provider.
    """

    def test_base_data_provider(self):
        TestDataProvider.register_data_provider_type(database=TEST_DATABASE)
        db_id = TestDataProvider.get_data_provider_type_db_id(
            database=TEST_DATABASE
        )
        self.assertIsNotNone(db_id)
        TestDataProvider.register_data_provider(
            'test_data_provider_1',
            database=TEST_DATABASE
        )
        test_data_provider = TestDataProvider(
            'test_data_provider_1',
            database=TEST_DATABASE,
        )
        self.assertIsNotNone(test_data_provider)
        self.assertIsNotNone(test_data_provider.db_id)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

