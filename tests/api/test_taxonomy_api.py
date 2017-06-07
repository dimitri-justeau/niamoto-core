# coding: utf-8

import unittest
import os
import logging

from geoalchemy2.shape import from_shape
from shapely.geometry import Point
import pandas as pd

from niamoto.testing import set_test_path
set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.db.connector import Connector
from niamoto.data_providers.base_occurrence_provider import \
    BaseOccurrenceProvider
from niamoto.data_providers.provider_types import PROVIDER_TYPES
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.test_data_provider import TestDataProvider
from niamoto.api import taxonomy_api
from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.exceptions import DataSourceNotFoundError


class TestTaxonomyApi(BaseTestNiamotoSchemaCreated):
    """
    Test case for taxonomy api.
    """

    TAXONOMY_CSV_PATH = os.path.join(
        NIAMOTO_HOME,
        'data',
        'taxonomy',
        'taxonomy_1.csv',
    )

    @classmethod
    def setUpClass(cls):
        super(TestTaxonomyApi, cls).setUpClass()

    def tearDown(self):
        with Connector.get_connection() as connection:
            with connection.begin():
                try:
                    TestDataProvider.unregister_data_provider(
                        'test_data_provider_a',
                        bind=connection
                    )
                except:
                    pass
                try:
                    TestDataProvider.unregister_data_provider(
                        'test_data_provider_b',
                        bind=connection
                    )
                except:
                    pass
                try:
                    TestDataProvider.unregister_data_provider_type(
                        bind=connection
                    )
                except:
                    pass
                TaxonomyManager.delete_all_taxa(bind=connection)
                TaxonomyManager.unregister_all_synonym_keys(bind=connection)

    def test_set_taxonomy(self):
        nb, synonyms = taxonomy_api.set_taxonomy(self.TAXONOMY_CSV_PATH)
        self.assertEqual(nb, 4)
        self.assertEqual(synonyms, {'gbif', 'taxref'})
        self.assertRaises(
            DataSourceNotFoundError,
            taxonomy_api.set_taxonomy,
            "fake_csv_file_path"
        )

    def test_map_all_synonyms(self):
        TestDataProvider.register_data_provider_type()
        PROVIDER_TYPES[TestDataProvider.get_type_name()] = TestDataProvider
        data_provider_a = TestDataProvider.register_data_provider(
            'test_data_provider_a',
        )
        data_provider_b = TestDataProvider.register_data_provider(
            'test_data_provider_b',
        )
        with Connector.get_connection() as connection:
            op_a = BaseOccurrenceProvider(data_provider_a)
            op_b = BaseOccurrenceProvider(data_provider_b)
            occ_a = pd.DataFrame.from_records([
                {
                    'id': 0,
                    'taxon_id': None,
                    'provider_taxon_id': 20,
                    'location': from_shape(Point(166.551, -22.039), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 1,
                    'taxon_id': None,
                    'provider_taxon_id': 30,
                    'location': from_shape(Point(166.551, -22.098), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 2,
                    'taxon_id': None,
                    'provider_taxon_id': 60,
                    'location': from_shape(Point(90.551, 14.098), srid=4326),
                    'properties': '{}',
                },
            ], index='id')
            occ_b = pd.DataFrame.from_records([
                {
                    'id': 0,
                    'taxon_id': None,
                    'provider_taxon_id': 2,
                    'location': from_shape(Point(160.551, -22.03), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 1,
                    'taxon_id': None,
                    'provider_taxon_id': 3,
                    'location': from_shape(Point(150.551, -20.98), srid=4326),
                    'properties': '{}',
                },
                {
                    'id': 2,
                    'taxon_id': None,
                    'provider_taxon_id': 4,
                    'location': from_shape(Point(166.55, -21.09), srid=4326),
                    'properties': '{}',
                },
            ], index='id')
            op_a._sync(occ_a, connection)
            op_b._sync(occ_b, connection)
            taxonomy_api.set_taxonomy(self.TAXONOMY_CSV_PATH)
            data_provider_a.update_data_provider(
                'test_data_provider_a',
                synonym_key='gbif'
            )
            data_provider_b.update_data_provider(
                'test_data_provider_b',
                synonym_key='taxref'
            )
        taxonomy_api.map_all_synonyms()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
