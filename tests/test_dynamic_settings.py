# coding: utf-8

import unittest

from niamoto.conf import DynamicSettings
from niamoto.testing import test_settings


class DynamicSettingTest(unittest.TestCase):

    def test_dynamic_settings(self):
        settings = DynamicSettings("niamoto.testing.test_settings")
        self.assertEqual(settings.NIAMOTO_SCHEMA, test_settings.NIAMOTO_SCHEMA)
        self.assertEqual(settings.DATABASES, test_settings.DATABASES)
        self.assertEqual(
            settings.DEFAULT_DATABASE,
            test_settings.DEFAULT_DATABASE
        )
        self.assertEqual(settings.TEST_DATABASE, test_settings.TEST_DATABASE)
        self.assertEqual(
            settings.DEFAULT_POSTGRES_SUPERUSER,
            test_settings.DEFAULT_POSTGRES_SUPERUSER
        )
        self.assertEqual(
            settings.DEFAULT_POSTGRES_SUPERUSER_PASSWORD,
            test_settings.DEFAULT_POSTGRES_SUPERUSER_PASSWORD
        )


if __name__ == '__main__':
    unittest.main()
