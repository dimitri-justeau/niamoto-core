# coding: utf-8

import unittest

from niamoto.testing import set_test_path
set_test_path()

from niamoto.db.schema_manager import SchemaManager
from niamoto.conf import settings
from niamoto.testing.base_tests import BaseTest
from niamoto.testing.test_database_manager import TestDatabaseManager


class TestSchemaManager(BaseTest):
    """
    Test case for schema manager.
    """

    def test_upgrade_db(self):
        SchemaManager.upgrade_db('head')

    def test_downgrade_db(self):
        SchemaManager.downgrade_db('base')

    def test_get_current_head(self):
        SchemaManager.get_current_revision()

    def test_get_current_revision(self):
        SchemaManager.get_head_revision()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
