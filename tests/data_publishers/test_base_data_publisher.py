# coding: utf-8

import unittest
import tempfile
import os

import pandas as pd
import geopandas as gpd
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import select, cast, String
from shapely.geometry import (
    Polygon,
    GeometryCollection,
    LineString,
    MultiPoint,
    MultiLineString,
    MultiPolygon
)

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.db.connector import Connector
from niamoto.db import metadata as meta
from niamoto.data_providers.csv_provider.csv_data_provider import \
    CsvDataProvider
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TEST_OCCURRENCE_CSV = os.path.join(
    NIAMOTO_HOME, 'data', 'csv', 'occurrences.csv',
)


class TestBaseDataPublisher(BaseTestNiamotoSchemaCreated):
    """
    Test for base data publisher.
    """

    def test_base_data_publisher(self):
        dp = BaseDataPublisher()
        temp_csv = tempfile.TemporaryFile(mode='w')
        data = pd.DataFrame.from_records([
            [1, 2, 3, 4],
            [5, 6, 7, 8]
        ])
        dp._publish_csv(data, destination=temp_csv)
        dp.publish(data, 'csv', destination=temp_csv)
        engine = Connector.get_engine()
        db_url = Connector.get_database_url()
        dp.publish(data, 'sql', destination='test_publish_table',
                   schema=settings.NIAMOTO_SCHEMA)
        dp.publish(data, 'sql', destination='test_publish_table',
                   db_url=db_url, if_exists='replace',
                   schema=settings.NIAMOTO_SCHEMA)
        inspector = Inspector.from_engine(engine)
        self.assertIn(
            'test_publish_table',
            inspector.get_table_names(schema=settings.NIAMOTO_SCHEMA),
        )
        temp_csv.close()

    def test_publish_to_postgis(self):
        CsvDataProvider.register_data_provider_type()
        CsvDataProvider.register_data_provider('csv_provider')
        csv_provider = CsvDataProvider(
            'csv_provider',
            occurrence_csv_path=TEST_OCCURRENCE_CSV,
        )
        csv_provider.sync()
        with Connector.get_connection() as connection:
            sel = select([
                 meta.occurrence.c.id.label('id'),
                 meta.occurrence.c.taxon_id.label('taxon_id'),
                 cast(meta.taxon.c.rank.label('rank'), String).label(
                     'rank'),
                 meta.taxon.c.full_name.label('full_name'),
                 cast(meta.occurrence.c.location, String).label(
                     'location'),
             ]).select_from(
                meta.occurrence.outerjoin(
                    meta.taxon,
                    meta.taxon.c.id == meta.occurrence.c.taxon_id
                )
            )
            df = gpd.read_postgis(
                sel,
                connection,
                index_col='id',
                geom_col='location',
                crs='+init=epsg:4326'
            )
            BaseDataPublisher._publish_sql(
                df,
                'test_export_postgis',
                schema='niamoto'
            )
            engine = Connector.get_engine()
            inspector = Inspector.from_engine(engine)
            self.assertIn(
                'test_export_postgis',
                inspector.get_table_names(
                    schema=settings.NIAMOTO_SCHEMA),
            )
            df2 = gpd.read_postgis(
                sel,
                connection,
                index_col='id',
                geom_col='location',
                crs={'init': 'epsg:4326'}
            )
            BaseDataPublisher._publish_sql(
                df2,
                'test_export_postgis',
                schema='niamoto',
                if_exists='truncate',
                truncate_cascade=True,
            )
            # Test geometry types
            polygon = Polygon([(0, 0), (1, 0), (1, 1)])
            linestring = LineString([(0, 0), (0, 1), (1, 1)])
            multipoint = MultiPoint([(1, 2), (3, 4), (5, 6)])
            multilinestring = MultiLineString(
                [[(1, 2), (3, 4), (5, 6)], [(7, 8), (9, 10)]]
            )
            polygon_2 = Polygon(
                [(1, 1), (1, -1), (-1, -1), (-1, 1)],
                [[(.5, .5), (.5, -.5), (-.5, -.5), (-.5, .5)]]
            )
            multipolygon = MultiPolygon([polygon, polygon_2])
            geometry = GeometryCollection([polygon, polygon_2])
            BaseDataPublisher._publish_sql(
                gpd.GeoDataFrame(
                    [{'A': 1, 'geom': polygon}],
                    geometry='geom'
                ),
                'test_export_postgis',
                schema='niamoto',
                if_exists='replace',
            )
            BaseDataPublisher._publish_sql(
                gpd.GeoDataFrame(
                    [{'A': 1, 'geom': linestring}],
                    geometry='geom'
                ),
                'test_export_postgis',
                schema='niamoto',
                if_exists='replace',
            )
            BaseDataPublisher._publish_sql(
                gpd.GeoDataFrame(
                    [{'A': 1, 'geom': multilinestring}],
                    geometry='geom'
                ),
                'test_export_postgis',
                schema='niamoto',
                if_exists='replace',
            )
            BaseDataPublisher._publish_sql(
                gpd.GeoDataFrame(
                    [{'A': 1, 'geom': multipoint}],
                    geometry='geom'
                ),
                'test_export_postgis',
                schema='niamoto',
                if_exists='replace',
            )
            BaseDataPublisher._publish_sql(
                gpd.GeoDataFrame(
                    [{'A': 1, 'geom': multipolygon}],
                    geometry='geom'
                ),
                'test_export_postgis',
                schema='niamoto',
                if_exists='replace',
            )
            BaseDataPublisher._publish_sql(
                gpd.GeoDataFrame(
                    [{'A': 1, 'geom': geometry}],
                    geometry='geom'
                ),
                'test_export_postgis',
                schema='niamoto',
                if_exists='replace',
            )
            BaseDataPublisher._publish_sql(
                gpd.GeoDataFrame(
                    [{'A': 1, 'geom': geometry}],
                    geometry='geom'
                ),
                'TEST123',
                schema='niamoto',
                if_exists='replace',
            )
            BaseDataPublisher._publish_sql(
                gpd.GeoSeries([polygon]),
                'test_export_postgis',
                schema='niamoto',
                if_exists='replace',
            )
            self.assertRaises(
                ValueError,
                BaseDataPublisher._publish_sql,
                gpd.GeoSeries([polygon]),
                'test_export_postgis',
                schema='niamoto',
                if_exists='thisisnotallowed',
            )



if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

