# coding: utf-8

import pandas as pd

from niamoto.conf import settings
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher


class RasterDataPublisher(BaseDataPublisher):
    """
    Publish rasters from the niamoto raster database.
    """

    @classmethod
    def get_key(cls):
        return 'raster'

    @classmethod
    def get_description(cls):
        return "Publish a raster from the niamoto raster database."

    @classmethod
    def get_publish_formats(cls):
        pass

    def _process(self, raster_name, *args, **kwargs):
        import rasterio
        pg_str = "dbname='{dbname}' " \
                 "host='{host}' " \
                 "port='{port}' " \
                 "user='{user}' " \
                 "password='{password}' " \
                 "schema='{schema}' " \
                 "table='{table}' " \
                 "mode='2'"
        pg_str = pg_str.format(**{
            'dbname': settings.NIAMOTO_DATABASE["NAME"],
            'host': settings.NIAMOTO_DATABASE["HOST"],
            'port': settings.NIAMOTO_DATABASE["PORT"],
            'user': settings.NIAMOTO_DATABASE["USER"],
            'password': settings.NIAMOTO_DATABASE["PASSWORD"],
            'schema': settings.NIAMOTO_RASTER_SCHEMA,
            'table': raster_name
        })
        pg_str = "PG:{}".format(pg_str)
        with rasterio.open(pg_str, driver="PostGISRaster") as dataset:
            return dataset.read(), [], []
