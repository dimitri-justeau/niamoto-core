# coding: utf-8

import unittest
import os

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.data_providers.plantnote_provider import *
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


class TestPlantnoteDataProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for plantnote data provider.
    """

    TEST_DB_PATH = os.path.join(
        NIAMOTO_HOME,
        'data',
        'plantnote',
        'ncpippn_test.sqlite',
    )

    def test_plantnote_data_provider(self):
        PlantnoteDataProvider.register_data_provider(
            'pl@ntnote_provider_1',
            self.TEST_DB_PATH,
        )
        test_data_provider = PlantnoteDataProvider(
            'pl@ntnote_provider_1',
            self.TEST_DB_PATH,
        )
        self.assertIsNotNone(test_data_provider)
        self.assertIsNotNone(test_data_provider.db_id)
        plot_provider = test_data_provider.plot_provider
        occurrence_provider = test_data_provider.occurrence_provider
        plot_occurrence_provider = test_data_provider.plot_occurrence_provider
        self.assertIsInstance(plot_provider, PlantnotePlotProvider)
        self.assertIsInstance(
            occurrence_provider,
            PlantnoteOccurrenceProvider
        )
        self.assertIsInstance(
            plot_occurrence_provider,
            PlantnotePlotOccurrenceProvider
        )
        # Test sync
        test_data_provider.sync()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

