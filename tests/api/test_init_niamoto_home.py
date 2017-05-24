# coding: utf-8

import unittest
import os
import shutil

from niamoto.testing import set_test_path
set_test_path()

from niamoto import conf
from niamoto.testing.base_tests import BaseTest
from niamoto.api.init_niamoto_home import init_niamoto_home


class TestInitNiamotoHomeApi(BaseTest):
    """
    Test case for init_niamoto_home api.
    """

    HOME_PATH = os.path.join(conf.NIAMOTO_HOME, "NEW_HOME")

    def tearDown(self):
        shutil.rmtree(self.HOME_PATH)

    @classmethod
    def setUpClass(cls):
        super(TestInitNiamotoHomeApi, cls).setUpClass()

    def test_init_niamoto_home(self):
        self.assertRaises(
            FileExistsError,
            init_niamoto_home,
            conf.NIAMOTO_HOME
        )
        init_niamoto_home(self.HOME_PATH)
        self.assertTrue(os.path.isdir(self.HOME_PATH))
        self.assertEqual(conf.NIAMOTO_HOME, self.HOME_PATH)


if __name__ == '__main__':
    unittest.main()
