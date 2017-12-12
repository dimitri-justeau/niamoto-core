# coding: utf-8

import unittest
import os
import logging
import tempfile

from niamoto.testing import set_test_path
set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.api.raster_api import add_raster
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.data_publishers.raster_data_publisher import \
    RasterDataPublisher, RasterValueCountPublisher
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TEST_RASTER = os.path.join(
    NIAMOTO_HOME,
    "data",
    "raster",
    "rainfall_wgs84.tif"
)


class TestRasterDataPublisher(BaseTestNiamotoSchemaCreated):
    """
    Test for raster publisher.
    """

    @classmethod
    def setUpClass(cls):
        super(TestRasterDataPublisher, cls).setUpClass()
        add_raster('test_raster', TEST_RASTER)

    def test_raster_publisher(self):
        publisher = RasterDataPublisher()
        raster_uri = publisher._process('test_raster')
        self.assertIn('tiff', publisher.get_publish_formats())
        tmp = tempfile.NamedTemporaryFile()
        publisher._publish_tiff(raster_uri, tmp.name)

    def test_raster_value_count_publisher(self):
        publisher = RasterValueCountPublisher()
        values = publisher._process('test_raster')


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

