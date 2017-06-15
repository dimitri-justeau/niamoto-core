# coding: utf-8

import unittest
import os
import logging

import pandas as pd

from niamoto.testing import set_test_path
set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.data_publishers.r_data_publisher import RDataPublisher
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.data_providers.csv_provider import CsvDataProvider
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TEST_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'occurrences.csv',
)

TEST_R_SCRIPT = os.path.join(
    NIAMOTO_HOME, 'R', 'r_script.R',
)


class TestRDataPublisher(BaseTestNiamotoSchemaCreated):
    """
    Test for occurrences publisher.
    """

    @classmethod
    def setUpClass(cls):
        super(TestRDataPublisher, cls).setUpClass()
        CsvDataProvider.register_data_provider_type()
        CsvDataProvider.register_data_provider('csv_provider')
        csv_provider = CsvDataProvider(
            'csv_provider',
            occurrence_csv_path=TEST_OCCURRENCE_CSV,
        )
        csv_provider.sync()

    def test_r_data_publisher(self):
        r_data_publisher = RDataPublisher(TEST_R_SCRIPT)
        result = r_data_publisher._process()[0]
        self.assertIsInstance(result, pd.DataFrame)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

