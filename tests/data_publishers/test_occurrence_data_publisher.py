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
from niamoto.data_publishers.occurrence_data_publisher import \
    OccurrenceDataPublisher, OccurrenceLocationPublisher
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.data_providers.csv_provider import CsvDataProvider
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TEST_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'occurrences.csv',
)


class TestOccurrencesPublisher(BaseTestNiamotoSchemaCreated):
    """
    Test for occurrences publisher.
    """

    @classmethod
    def setUpClass(cls):
        super(TestOccurrencesPublisher, cls).setUpClass()
        CsvDataProvider.register_data_provider('csv_provider')
        csv_provider = CsvDataProvider(
            'csv_provider',
            occurrence_csv_path=TEST_OCCURRENCE_CSV,
        )
        csv_provider.sync()

    def test_occurrences_publisher(self):
        op = OccurrenceDataPublisher()
        op.process()
        result = op.process(drop_null_properties=True)
        self.assertEqual(len(result), 3)
        self.assertIsNotNone(op.get_key())
        self.assertIsNotNone(op.get_publish_formats())

    def test_occurrence_locations_publisher(self):
        op = OccurrenceLocationPublisher()
        result = op.process()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

