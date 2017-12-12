# coding: utf-8

import unittest
import os
import logging

import geopandas as gpd

from niamoto.testing import set_test_path
set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.api.vector_api import add_vector
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.data_publishers.vector_publisher import VectorDataPublisher
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


SHP_TEST = os.path.join(
    NIAMOTO_HOME, 'data', 'vector', 'NCL_adm', 'NCL_adm1.shp'
)


class TestVectorDataPublisher(BaseTestNiamotoSchemaCreated):
    """
    Test for vector publisher.
    """

    @classmethod
    def setUpClass(cls):
        super(TestVectorDataPublisher, cls).setUpClass()
        add_vector('test_vector', SHP_TEST)

    def test_vector_publisher(self):
        publisher = VectorDataPublisher()
        data = publisher.process('test_vector')[0]
        self.assertIsInstance(data, gpd.GeoDataFrame)


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

