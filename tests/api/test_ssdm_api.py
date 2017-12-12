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
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.api import ssdm_api
from niamoto.exceptions import NoRecordFoundError
from niamoto.taxonomy import populate


class TestSSDMApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for ssdm api.
    """

    TEST_SDM_1038 = os.path.join(
        NIAMOTO_HOME,
        "data",
        "sdm",
        "1038",
        "Rasters",
        "Probability.tif"
    )
    TEST_SDM_1180 = os.path.join(
        NIAMOTO_HOME,
        "data",
        "sdm",
        "1180",
        "Rasters",
        "Probability.tif"
    )

    @classmethod
    def setUpClass(cls):
        super(TestSSDMApi, cls).setUpClass()
        # Populate taxon database
        populate.populate_ncpippn_taxon_database(
            populate.load_ncpippn_taxon_dataframe_from_json(),
        )

    def tearDown(self):
        delete_stmt = niamoto_db_meta.sdm_registry.delete()
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_SSDM_SCHEMA
            )
            for tb in tables:
                if tb != niamoto_db_meta.raster_registry.name:
                    connection.execute("DROP TABLE IF EXISTS {};".format(
                        "{}.{}".format(settings.NIAMOTO_SSDM_SCHEMA, tb)
                    ))
            connection.execute(delete_stmt)

    def test_get_sdm_list(self):
        df1 = ssdm_api.get_sdm_list()
        self.assertEqual(len(df1), 0)

    def test_add_sdm(self):
        ssdm_api.add_sdm(
            1180,
            self.TEST_SDM_1180,
            tile_dimension=(200, 200),
        )

    def test_update_raster(self):
        ssdm_api.add_sdm(
            1180,
            self.TEST_SDM_1180,
            tile_dimension=(200, 200),
        )
        ssdm_api.update_sdm(
            1180,
            tile_dimension=(150, 150),
        )

    def test_delete_raster(self):
        ssdm_api.add_sdm(
            1180,
            self.TEST_SDM_1180,
        )
        ssdm_api.delete_sdm(1180)
        self.assertRaises(
            NoRecordFoundError,
            ssdm_api.delete_sdm,
            1180,
        )

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_SSDM_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
