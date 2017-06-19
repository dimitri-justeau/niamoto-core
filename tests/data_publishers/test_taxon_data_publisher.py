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
from niamoto.api.taxonomy_api import set_taxonomy
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.data_publishers.taxon_data_publisher import TaxonDataPublisher
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TAXONOMY_CSV_PATH = os.path.join(
    NIAMOTO_HOME,
    'data',
    'taxonomy',
    'taxonomy_1.csv',
)


class TestOccurrencesPublisher(BaseTestNiamotoSchemaCreated):
    """
    Test for occurrences publisher.
    """

    @classmethod
    def setUpClass(cls):
        super(TestOccurrencesPublisher, cls).setUpClass()
        set_taxonomy(TAXONOMY_CSV_PATH)

    def test_occurrences_publisher(self):
        publisher = TaxonDataPublisher()
        result = publisher.process(include_mptt=True)
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

