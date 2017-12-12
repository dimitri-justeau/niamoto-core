# coding: utf-8

import unittest
import os
from datetime import datetime
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
from niamoto.ssdm.ssdm_manager import SSDMManager
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.taxonomy import populate
from niamoto.exceptions import NoRecordFoundError


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


class TestSSDMManager(BaseTestNiamotoSchemaCreated):
    """
    Test case for SSDMManager class.
    """

    @classmethod
    def setUpClass(cls):
        super(TestSSDMManager, cls).setUpClass()
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
        df1 = SSDMManager.get_sdm_list()
        self.assertEqual(len(df1), 0)
        data = [
            {
                'name': 'species_1038',
                'taxon_id': 1038,
                'date_create': datetime.now(),
                'date_update': datetime.now(),
                'properties': {},
            },
            {
                'name': 'species_1180',
                'taxon_id': 1180,
                'date_create': datetime.now(),
                'date_update': datetime.now(),
                'properties': {},
            },
            {
                'name': 'species_1260',
                'taxon_id': 1260,
                'date_create': datetime.now(),
                'date_update': datetime.now(),
                'properties': {},
            },
        ]
        ins = niamoto_db_meta.sdm_registry.insert().values(data)
        with Connector.get_connection() as connection:
            connection.execute(ins)
        df2 = SSDMManager.get_sdm_list()
        self.assertEqual(len(df2), 3)

    def test_add_sdm(self):
        #  Test non existing raster
        null_path = os.path.join(NIAMOTO_HOME, "NULL.tif")
        self.assertRaises(
            FileNotFoundError,
            SSDMManager.add_sdm,
            1038, null_path,
            tile_dimension=(200, 200)
        )
        # Test wrong taxon id
        self.assertRaises(
            NoRecordFoundError,
            SSDMManager.add_sdm,
            -1,
            TEST_SDM_1038,
        )
        # Test existing raster
        SSDMManager.add_sdm(
            1038,
            TEST_SDM_1038,
        )
        df = SSDMManager.get_raster_list()
        self.assertEqual(len(df), 1)
        self.assertEqual(df['name'].iloc[0], 'species_{}'.format(1038))
        engine = Connector.get_engine()
        inspector = Inspector.from_engine(engine)
        self.assertIn(
            'species_{}'.format(1038),
            inspector.get_table_names(schema=settings.NIAMOTO_SSDM_SCHEMA),
        )

    def test_update_sdm(self):
        SSDMManager.add_sdm(
            1038,
            TEST_SDM_1038,
        )
        # Update raster
        SSDMManager.update_sdm(
            1038,
            TEST_SDM_1180,
        )

    def test_delete_raster(self):
        SSDMManager.add_sdm(
            1038,
            TEST_SDM_1038,
        )
        self.assertEqual(len(SSDMManager.get_sdm_list()), 1)
        SSDMManager.delete_sdm(1038)
        self.assertEqual(len(SSDMManager.get_sdm_list()), 0)
        engine = Connector.get_engine()
        inspector = Inspector.from_engine(engine)
        self.assertNotIn(
            'species_{}'.format(1038),
            inspector.get_table_names(schema=settings.NIAMOTO_SSDM_SCHEMA),
        )


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_SSDM_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
