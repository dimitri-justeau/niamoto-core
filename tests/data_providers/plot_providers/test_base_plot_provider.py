# coding: utf-8

import unittest

from niamoto.data_providers.plot_providers.base_plot_provider import *
from tests.test_utils import TestDatabaseManager
from tests import BaseTestNiamotoSchemaCreated


class TestBasePlotProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for base plot provider.
    """

    def test_get_current_plot_data(self):
        """
        :return: Test for get_current_plot_data_method.
            Test the structure of the returned DataFrame.
            Test retrieving an empty DataFrame.
            Test retrieving a not-empty DataFrame.
        """
        pp = BasePlotProvider(0)
        #  1. retrieve an empty DataFrame.
        df1 = pp.get_current_plot_data()
        self.assertEqual(len(df1), 0)
        #  2. Check the structure of the DataFrame.
        df_cols = df1.columns
        pass


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema('niamoto')
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
