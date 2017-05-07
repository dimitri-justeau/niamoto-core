# coding: utf-8

import unittest

from niamoto.settings.dynamic_settings import DynamicSettings


class DynamicSettingTest(unittest.TestCase):

    def test_dynamic_settings(self):
        settings = DynamicSettings(
            "niamoto.testing.test_niamoto_home.settings"
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
