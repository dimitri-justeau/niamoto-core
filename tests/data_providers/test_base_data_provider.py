# coding: utf-8

import unittest

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.taxonomy.taxonomy_manager import TaxonomyManager


class TestBaseDataProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for base data provider.
    """

    def test_base_data_provider(self):
        TestDataProvider.register_data_provider_type()
        db_id = TestDataProvider.get_data_provider_type_db_id()
        self.assertIsNotNone(db_id)
        TestDataProvider.register_data_provider('test_data_provider_1')
        test_data_provider = TestDataProvider('test_data_provider_1')
        self.assertIsNotNone(test_data_provider)
        self.assertIsNotNone(test_data_provider.db_id)
        # Test with synonym key
        TaxonomyManager.register_synonym_key("synonym")
        TestDataProvider.register_data_provider(
            'test_data_provider_2',
            synonym_key="synonym"
        )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

