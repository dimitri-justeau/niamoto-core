# coding: utf-8

import unittest

from niamoto.data_providers.base_data_provider import BaseDataProvider
from niamoto.settings import TEST_DATABASE
from tests.test_utils import TestDatabaseManager
from tests import BaseTestNiamotoSchemaCreated


class TestDataProvider(BaseDataProvider):

    @property
    def plot_provider(self):
        raise NotImplementedError()

    @property
    def occurrence_provider(self):
        raise NotImplementedError()

    @classmethod
    def get_type_name(cls):
        return "TEST_DATA_PROVIDER"


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

