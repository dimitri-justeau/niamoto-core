# coding: utf-8

import unittest
import os

from geoalchemy2.shape import from_shape, WKTElement
from shapely.geometry import Point

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated
from niamoto.testing.test_database_manager import TestDatabaseManager


class TestPlantnoteOccurrenceProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for plantnote occurrence provider.
    """

    TEST_DB_PATH = os.path.join(
        NIAMOTO_HOME,
        'data',
        'plantnote',
        'ncpippn.sqlite',
    )

    @classmethod
    def setUpClass(cls):
        super(TestPlantnoteOccurrenceProvider, cls).setUpClass()
        PlantnoteDataProvider.register_data_provider_type(
            database=settings.TEST_DATABASE
        )
        PlantnoteDataProvider.register_data_provider(
            'pl@ntnote_provider',
            cls.TEST_DB_PATH,
            database=settings.TEST_DATABASE,
        )

    def test_get_provider_occurrence_dataframe(self):
        pt_provider = PlantnoteDataProvider(
            'pl@ntnote_provider',
            self.TEST_DB_PATH,
            database=settings.TEST_DATABASE,
        )
        occ_provider = pt_provider.occurrence_provider
        df1 = occ_provider.get_provider_occurrence_dataframe()
        # print(df1[:10])


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
