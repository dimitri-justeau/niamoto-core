# coding: utf-8

import unittest

from niamoto.db.connector import Connector
from niamoto.db.metadata import metadata
from niamoto.db.schema_manager import SchemaManager
from niamoto.testing.test_database_manager import TestDatabaseManager


class BaseTest(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        Connector.dispose_engines()


class BaseTestNiamotoSchemaCreated(BaseTest):

    @classmethod
    def setUpClass(cls):
        engine = Connector.get_engine()
        metadata.create_all(engine)

    @classmethod
    def tearDownClass(cls):
        engine = Connector.get_engine()
        metadata.drop_all(engine)
        super(BaseTestNiamotoSchemaCreated, cls).tearDownClass()
