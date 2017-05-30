# coding: utf-8

import unittest
import os

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.exceptions import DataSourceNotFoundError, \
    MalformedDataSourceError
from niamoto.data_providers.csv_provider import CsvDataProvider
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


DB = settings.TEST_DATABASE

TEST_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME,
    'data',
    'csv',
    'occurrences.csv',
)
TEST_NO_PROPERTIES_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME,
    'data',
    'csv',
    'occurrences_no_properties.csv',
)
TEST_MALFORMED_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME,
    'data',
    'csv',
    'occurrences_malformed.csv',
)
TEST_PLOT_CSV = os.path.join(
    NIAMOTO_HOME,
    'data',
    'csv',
    'plots.csv',
)
TEST_MALFORMED_PLOT_CSV = os.path.join(
    NIAMOTO_HOME,
    'data',
    'csv',
    'plots_malformed.csv',
)
TEST_PLOT_OCC_CSV = os.path.join(
    NIAMOTO_HOME,
    'data',
    'csv',
    'plots_occurrences.csv',
)
TEST_MALFORMED_PLOT_OCC_CSV = os.path.join(
    NIAMOTO_HOME,
    'data',
    'csv',
    'plots_occurrences_malformed.csv',
)


class TestCsvDataProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for Csv data provider.
    """

    def test_csv_data_provider(self):
        CsvDataProvider.register_data_provider_type(database=DB)
        CsvDataProvider.register_data_provider('csv_provider', database=DB)
        # Test with no files
        csv_provider = CsvDataProvider('csv_provider', database=DB)
        csv_provider.sync()
        # Test with occurrences
        csv_provider = CsvDataProvider(
            'csv_provider',
            database=DB,
            occurrence_csv_path=TEST_OCCURRENCE_CSV,
        )
        csv_provider.sync()
        self.assertRaises(
            DataSourceNotFoundError,
            CsvDataProvider,
            'csv_provider',
            database=DB,
            occurrence_csv_path='YO'
        )
        csv_provider = CsvDataProvider(
            'csv_provider',
            database=DB,
            occurrence_csv_path=TEST_MALFORMED_OCCURRENCE_CSV,
        )
        self.assertRaises(
            MalformedDataSourceError,
            csv_provider.sync,
        )
        csv_provider = CsvDataProvider(
            'csv_provider',
            database=DB,
            occurrence_csv_path=TEST_NO_PROPERTIES_OCCURRENCE_CSV,
        )
        csv_provider.sync()
        # Test with plots
        csv_provider = CsvDataProvider(
            'csv_provider',
            database=DB,
            plot_csv_path=TEST_PLOT_CSV,
        )
        csv_provider.sync()
        self.assertRaises(
            DataSourceNotFoundError,
            CsvDataProvider,
            'csv_provider',
            database=DB,
            plot_csv_path='YO'
        )
        csv_provider = CsvDataProvider(
            'csv_provider',
            database=DB,
            plot_csv_path=TEST_MALFORMED_PLOT_CSV,
        )
        self.assertRaises(
            MalformedDataSourceError,
            csv_provider.sync,
        )
        # Test with plots / occurrences


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

