# coding: utf-8

import unittest
import tempfile

from click.testing import CliRunner

from niamoto.testing import set_test_path

set_test_path()

from niamoto.conf import settings
from niamoto.bin.commands import publish
from niamoto.data_publishers.occurrence_data_publisher import \
    OccurrenceDataPublisher
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


class TestCLIPublish(BaseTestNiamotoSchemaCreated):
    """
    Test case for publish cli methods.
    """

    def test_list_publishers_cli(self):
        runner = CliRunner()
        result = runner.invoke(
            publish.list_publishers_cli,
            []
        )
        self.assertEqual(result.exit_code, 0)

    def test_publish_cli(self):
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode='w') as csv_temp:
            result = runner.invoke(
                publish.publish_cli,
                [
                    OccurrenceDataPublisher.get_key(),
                    BaseDataPublisher.CSV,
                    '-d', csv_temp.name
                ]
            )
            self.assertEqual(result.exit_code, 0)
            result = runner.invoke(
                publish.publish_cli,
                [
                    OccurrenceDataPublisher.get_key(),
                    BaseDataPublisher.CSV,
                    '-d', csv_temp.name,
                    "--properties", "dbh,height"
                ]
            )
            self.assertEqual(result.exit_code, 0)
            result = runner.invoke(
                publish.publish_cli,
                [
                    "R_r_script",
                    BaseDataPublisher.CSV,
                    '-d', csv_temp.name,
                    "--properties", "dbh,height"
                ]
            )
            self.assertEqual(result.exit_code, 0)
            result = runner.invoke(
                publish.publish_cli,
                [
                    "yo",
                    BaseDataPublisher.CSV,
                    '-d', csv_temp.name,
                    "--properties", "dbh,height"
                ]
            )
            self.assertEqual(result.exit_code, 1)

if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()
