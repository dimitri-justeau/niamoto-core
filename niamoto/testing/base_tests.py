# coding: utf-8

import unittest

from sqlalchemy.engine.reflection import Inspector

from niamoto.conf import settings
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta


class BaseTest(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        Connector.dispose_engines()


class BaseTestNiamotoSchemaCreated(BaseTest):

    @classmethod
    def setUpClass(cls):
        engine = Connector.get_engine()
        meta.metadata.create_all(engine, tables=[
            meta.occurrence,
            meta.plot,
            meta.plot_occurrence,
            meta.data_provider,
            meta.data_provider_type,
            meta.taxon,
            meta.synonym_key_registry,
            meta.raster_registry,
            meta.vector_registry,
            meta.dimension_registry,
            meta.fact_table_registry,
        ])

    @classmethod
    def tearDownClass(cls):
        engine = Connector.get_engine()
        meta.metadata.drop_all(engine)
        with Connector.get_connection() as connection:
            inspector = Inspector.from_engine(connection)
            # Drop vectors
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_VECTOR_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE IF EXISTS {} CASCADE;".format(
                    "{}.{}".format(settings.NIAMOTO_VECTOR_SCHEMA, tb)
                ))
            # Drop rasters
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_RASTER_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE IF EXISTS {} CASCADE;".format(
                    "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, tb)
                ))
            # Drop fact tables
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_FACT_TABLES_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE IF EXISTS {} CASCADE;".format(
                    "{}.{}".format(settings.NIAMOTO_FACT_TABLES_SCHEMA, tb)
                ))
            # Drop dimensions
            tables = inspector.get_table_names(
                schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
            )
            for tb in tables:
                connection.execute("DROP TABLE IF EXISTS {} CASCADE;".format(
                    "{}.{}".format(settings.NIAMOTO_DIMENSIONS_SCHEMA, tb)
                ))
        super(BaseTestNiamotoSchemaCreated, cls).tearDownClass()
