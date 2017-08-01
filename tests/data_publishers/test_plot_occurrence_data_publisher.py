# coding: utf-8

import unittest
import os
import logging

from niamoto.testing import set_test_path
set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.data_publishers.plot_occurrence_data_publisher import \
    PlotOccurrenceDataPublisher
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.data_providers.csv_provider import CsvDataProvider
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TEST_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'occurrences.csv',
)
TEST_PLOT_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'plots.csv',
)
TEST_PLOT_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'plots_occurrences.csv',
)


class TestPlotOccurrencePublisher(BaseTestNiamotoSchemaCreated):
    """
    Test for plot occurrence publisher.
    """

    @classmethod
    def setUpClass(cls):
        super(TestPlotOccurrencePublisher, cls).setUpClass()
        CsvDataProvider.register_data_provider('csv_provider')
        csv_provider = CsvDataProvider(
            'csv_provider',
            occurrence_csv_path=TEST_OCCURRENCE_CSV,
            plot_csv_path=TEST_PLOT_CSV,
            plot_occurrence_csv_path=TEST_PLOT_OCCURRENCE_CSV
        )
        csv_provider.sync()

    def test_plot_occurrence_publisher(self):
        publisher = PlotOccurrenceDataPublisher()
        result = publisher.process()
        self.assertEqual(len(result), 3)
        self.assertIsNotNone(publisher.get_key())
        self.assertIsNotNone(publisher.get_publish_formats())


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

