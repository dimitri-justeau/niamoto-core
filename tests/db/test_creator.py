# coding: utf-8

import unittest

from niamoto.testing import set_test_path
set_test_path()

from niamoto.db.connector import Connector
from niamoto.db.creator import Creator
from niamoto.conf import settings
from niamoto.testing.base_tests import BaseTest
from niamoto.testing.test_database_manager import TestDatabaseManager


class TestCreator(BaseTest):
    """
    Test case for db creator.
    """

    def test_create_drop_niamoto_schema(self):
        engine = Connector.get_engine(
            database=settings.TEST_DATABASE,
        )
        Creator.create_niamoto_schema(engine)
        Creator.drop_niamoto_schema(engine)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
