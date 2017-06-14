# coding: utf-8

import unittest
import tempfile

import pandas as pd

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


class TestBaseDataPublisher(BaseTestNiamotoSchemaCreated):
    """
    Test for base data publisher.
    """

    def test_base_data_publisher(self):
        dp = BaseDataPublisher()
        temp_csv = tempfile.TemporaryFile(mode='w')
        data = pd.DataFrame.from_records([
            [1, 2, 3, 4],
            [5, 6, 7, 8]
        ])
        dp._publish_csv(data, temp_csv)
        dp.publish(data, 'csv', temp_csv)
        temp_csv.close()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

