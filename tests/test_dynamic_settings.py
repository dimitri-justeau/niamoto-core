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
        self.assertEqual(settings.NIAMOTO_SCHEMA, settings.NIAMOTO_SCHEMA)
        self.assertEqual(settings.DATABASES, settings.DATABASES)
        self.assertEqual(
            settings.DEFAULT_DATABASE,
            settings.DEFAULT_DATABASE
        )
        self.assertEqual(settings.TEST_DATABASE, settings.TEST_DATABASE)
        self.assertEqual(
            settings.DEFAULT_POSTGRES_SUPERUSER,
            settings.DEFAULT_POSTGRES_SUPERUSER
        )
        self.assertEqual(
            settings.DEFAULT_POSTGRES_SUPERUSER_PASSWORD,
            settings.DEFAULT_POSTGRES_SUPERUSER_PASSWORD
        )


if __name__ == '__main__':
    unittest.main()
