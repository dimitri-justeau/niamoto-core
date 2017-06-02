# coding: utf-8

import os
import sys
import unittest

from niamoto.dynamic_settings import DynamicSettings


class DynamicSettingTest(unittest.TestCase):

    def test_dynamic_settings(self):
        path = os.path.abspath(os.path.dirname(__file__))
        sys.path.append(os.path.join(
            path, "test_data", "test_niamoto_home"
        ))
        settings = DynamicSettings(
            os.path.join(
                path, "test_data", "test_niamoto_home", "settings.py"
            )
        )
        self.assertIsNotNone(settings)

if __name__ == '__main__':
    unittest.main()
