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

    def _process(self, raster_name, *args, cuts=None, **kwargs):
        """
        :param raster_name: The name of the raster.
        :param cuts: Cuts corresponding to value categories: [(range, label)].
            e.g: [
                ((0, 10), 'low'),        => [0, 10[
                ((10, 20), 'medium'),    => [10, 20[
                ((20, 30), 'high')       => [20, 30[
            ]
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
            if cuts is not None:
                df['category'] = ''
                cut_values = cuts[0]
                labels = cuts[1]
                for i, label in enumerate(labels):
                    if i == 0:
                        cond = df[raster_name] < cut_values[0]
                    elif i == (len(labels) - 1):
                        cond = df[raster_name] >= cut_values[len(labels) - 2]
                    else:
                        x1 = cut_values[i - 1]
                        x2 = cut_values[i]
                        cond1 = df[raster_name] >= x1
                        cond2 = df[raster_name] < x2
                        cond = cond1 & cond2
                    df.loc[cond, 'category'] = label
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
