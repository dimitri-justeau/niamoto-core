# coding: utf-8

import unittest
import tempfile

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.exceptions import WrongPublisherKeyError, \
    UnavailablePublishFormat
from niamoto.api import publish_api
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.data_publishers.occurrence_data_publisher import \
    OccurrenceDataPublisher


class TestPublishApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for publish api.
    """

    @classmethod
    def setUpClass(cls):
        super(TestPublishApi, cls).setUpClass()

    def test_publish(self):
        self.assertRaises(
            WrongPublisherKeyError,
            publish_api.publish,
            'wrong_key',
            'csv',
            'yo'
        )
        self.assertRaises(
            UnavailablePublishFormat,
            publish_api.publish,
            OccurrenceDataPublisher.get_key(),
            "Yo",
            "Yo"
        )
        csv_temp = tempfile.TemporaryFile(mode='w')
        publish_api.publish(
            OccurrenceDataPublisher.get_key(),
            BaseDataPublisher.CSV,
            csv_temp
        )
        csv_temp.close()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
