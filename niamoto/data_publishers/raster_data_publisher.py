# coding: utf-8

import subprocess

import pandas as pd

from niamoto.conf import settings
from niamoto.db.connector import Connector
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


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
        return [cls.TIFF]

    def _process(self, raster_name, *args, **kwargs):
        """
        :param raster_name: The raster name in Niamoto raster database.
        :return: The raster PG string, usable from the GDAL PG Driver.

        """
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
        return "PG:{}".format(pg_str)

    @staticmethod
    def _publish_tiff(data, destination, *args, **kwargs):
        """
        Publish the raster in a tiff file.
        The data argument must be a valid GDAL url.
        :param destination:
        """
        LOGGER.debug("RasterDataPublisher._publish_tiff({}, {})".format(
            data, destination
        ))
        p = subprocess.Popen([
            'gdal_translate', '-of', 'GTiff', data, destination
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if stdout:
            LOGGER.debug(stdout)
        if stderr:
            LOGGER.debug(stderr)

    format_to_methods = BaseDataPublisher.FORMAT_TO_METHOD
    FORMAT_TO_METHOD = format_to_methods
    FORMAT_TO_METHOD[BaseDataPublisher.TIFF] = _publish_tiff.__func__


class RasterValueCountPublisher(BaseDataPublisher):
    """
    Publish the distinct values of a raster and the pixel count.
    """

    def _process(self, raster_name, *args, **kwargs):
        """
        :param raster_name: The name of the raster.
        :return: A DataFrame containing the distinct values of the raster
            and the associated pixel count
        """
        sql = \
            """
            SELECT (value_count).VALUE AS {raster_name},
                SUM((value_count).COUNT) AS pixel_count
            FROM (
                SELECT ST_ValueCount(rast, 1) AS value_count
                FROM {raster_schema}.{raster_name}
            ) AS val
            GROUP BY {raster_name}
            ORDER BY {raster_name};
            """.format(**{
                'raster_name': raster_name,
                'raster_schema': settings.NIAMOTO_RASTER_SCHEMA
            })
        with Connector.get_connection() as connection:
            df = pd.read_sql(sql, connection)
            return df

    @classmethod
    def get_description(cls):
        return "Publish the distinct values of a raster and the " \
               "associated pixel count."

    @classmethod
    def get_key(cls):
        return "raster_value_count"

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.SQL]
