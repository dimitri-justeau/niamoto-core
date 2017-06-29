# coding: utf-8

import subprocess

from niamoto.conf import settings
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
        Return the raster PG string, usable from the GDAL PG Driver.
        :param raster_name:
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
    def _publish_tiff(raster_uri, destination, *args, **kwargs):
        LOGGER.debug("RasterDataPublisher._publish_tiff({}, {})".format(
            raster_uri, destination
        ))
        p = subprocess.Popen([
            'gdal_translate', '-of', 'GTiff', raster_uri, destination
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if stdout:
            LOGGER.debug(stdout)
        if stderr:
            LOGGER.debug(stderr)

    format_to_methods = BaseDataPublisher.FORMAT_TO_METHOD
    FORMAT_TO_METHOD = format_to_methods
    FORMAT_TO_METHOD[BaseDataPublisher.TIFF] = _publish_tiff.__func__
