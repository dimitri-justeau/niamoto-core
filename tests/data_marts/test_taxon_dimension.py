# coding: utf-8

import os
import unittest

from sqlalchemy.engine.reflection import Inspector

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.data_marts.dimensions.taxon_dimension import TaxonDimension
from niamoto.api.taxonomy_api import set_taxonomy
from niamoto.db.connector import Connector


TAXONOMY_CSV_PATH = os.path.join(
    NIAMOTO_HOME,
    'data',
    'taxonomy',
    'taxonomy_1.csv',
)


class TestTaxonDimension(BaseTestNiamotoSchemaCreated):
    """
    Test case for TaxonDimension class.
    """

    @classmethod
    def setUpClass(cls):
        super(TestTaxonDimension, cls).setUpClass()
        set_taxonomy(TAXONOMY_CSV_PATH)

    def tearDown(self):
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE IF EXISTS {};".format(
                    "{}.{}".format(settings.NIAMOTO_DIMENSIONS_SCHEMA, tb)
                ))

    def test_taxon_dimension(self):
        dim = TaxonDimension()
        dim.create_dimension()
        dim.populate_from_publisher()
        dim.get_values()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
