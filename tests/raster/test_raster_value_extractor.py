# coding: utf-8

import unittest
import os
import logging

from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.raster.raster_manager import RasterManager
from niamoto.raster.raster_value_extractor import RasterValueExtractor
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.data_providers.csv_provider import CsvDataProvider


TEST_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'occurrences.csv',
)


class TestRasterValueExtractor(BaseTestNiamotoSchemaCreated):
    """
    Test case for RasterValueExtractor class.
    """

    @classmethod
    def setUpClass(cls):
        super(TestRasterValueExtractor, cls).setUpClass()
        CsvDataProvider.register_data_provider_type()
        CsvDataProvider.register_data_provider('csv_provider')
        csv_provider = CsvDataProvider(
            'csv_provider',
            occurrence_csv_path=TEST_OCCURRENCE_CSV,
        )
        csv_provider.sync()

    def tearDown(self):
        delete_stmt = niamoto_db_meta.raster_registry.delete()
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_RASTER_SCHEMA
            )
            for tb in tables:
                if tb != niamoto_db_meta.raster_registry.name:
                    connection.execute("DROP TABLE IF EXISTS {};".format(
                        "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, tb)
                    ))
            connection.execute(delete_stmt)

    def test_extract_raster_values_to_occurrences(self):
        # Test existing raster
        test_raster = os.path.join(
            NIAMOTO_HOME,
            "data",
            "raster",
            "rainfall_wgs84.tif"
        )
        RasterManager.add_raster(
            test_raster,
            "rainfall",
        )
        RasterValueExtractor.extract_raster_values_to_occurrences('rainfall')


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
