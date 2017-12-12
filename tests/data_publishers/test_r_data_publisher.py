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
from niamoto.raster.raster_manager import RasterManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TEST_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'occurrences.csv',
)

TEST_PLOT_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'plots.csv',
)

TEST_PLOTS_OCCURRENCES_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'plots_occurrences.csv',
)

TEST_R_SCRIPT = os.path.join(
    NIAMOTO_HOME, 'R', 'r_script.R',
)

TEST_RASTER = os.path.join(
    NIAMOTO_HOME,
    "data",
    "raster",
    "rainfall_wgs84.tif"
)


class TestRDataPublisher(BaseTestNiamotoSchemaCreated):
    """
    Test for occurrences publisher.
    """

    def test_r_data_publisher(self):
        #  Test with empty dataframes
        r_data_publisher = RDataPublisher(TEST_R_SCRIPT)
        result = r_data_publisher._process()[0]
        self.assertIsInstance(result, pd.DataFrame)
        #  Add data
        CsvDataProvider.register_data_provider('csv_provider')
        csv_provider = CsvDataProvider(
            'csv_provider',
            occurrence_csv_path=TEST_OCCURRENCE_CSV,
            plot_csv_path=TEST_PLOT_CSV,
            plot_occurrence_csv_path=TEST_PLOTS_OCCURRENCES_CSV
        )
        csv_provider.sync()
        RasterManager.add_raster('test_raster', TEST_RASTER)
        result = r_data_publisher._process()[0]
        self.assertIsInstance(result, pd.DataFrame)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

