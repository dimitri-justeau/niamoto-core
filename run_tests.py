# coding: utf-8

import unittest
import sys
import logging

from niamoto.testing import set_test_path
set_test_path()

from niamoto import log

log.STREAM_LOGGING_LEVEL = logging.CRITICAL
log.FILE_LOGGING_LEVEL = logging.DEBUG

for handler in logging.getLogger('niamoto').handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setLevel(logging.CRITICAL)


if __name__ == "__main__":
    from niamoto.testing.test_database_manager import TestDatabaseManager
    from niamoto.conf import settings
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_DIMENSIONS_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_FACT_TABLES_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_SSDM_SCHEMA)
    test_suite = unittest.TestLoader().discover('tests')
    test_results = unittest.TextTestRunner(verbosity=1).run(test_suite)
    TestDatabaseManager.teardown_test_database()
    if len(test_results.failures) > 0 or len(test_results.errors) > 0:
        sys.exit(1)
