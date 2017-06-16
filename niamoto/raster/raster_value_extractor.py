# coding: utf-8

from niamoto.conf import settings
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector
from niamoto.raster.raster_manager import RasterManager
from niamoto.log import get_logger


LOGGER = get_logger(__name__)

RASTER_PROPERTY_PREFIX = ""


class RasterValueExtractor:
    """
    Class managing the value extraction from rasters to occurrences and plots.
    """

    @classmethod
    def extract_raster_values_to_occurrences(cls, raster_name):
        with Connector.get_connection() as connection:
            with connection.begin():
                RasterManager.assert_raster_exists(
                    raster_name,
                    connection=connection
                )
                m = "Extracting '{}' raster values to occurrences properties."
                LOGGER.debug(m.format(raster_name))
                sql = \
                    """
                    WITH {raster_name} AS (
                      SELECT occ.id AS id,
                        ST_Value(raster.rast, occ.location) AS rast_value
                      FROM {occurrence_table} AS occ
                      LEFT JOIN {raster_table} AS raster
                      ON ST_Intersects(raster.rast, occ.location)
                    )
                    UPDATE {occurrence_table}
                    SET properties = (
                      {occurrence_table}.properties || jsonb_build_object(
                        '{prefix}{raster_name}', {raster_name}.rast_value
                      )
                    ) FROM {raster_name}
                    WHERE {raster_name}.id = {occurrence_table}.id
                    """.format(**{
                        'raster_name': raster_name,
                        'raster_table': '{}.{}'.format(
                            settings.NIAMOTO_RASTER_SCHEMA,
                            raster_name
                        ),
                        'occurrence_table': '{}.{}'.format(
                            settings.NIAMOTO_SCHEMA,
                            meta.occurrence.name
                        ),
                        'prefix': RASTER_PROPERTY_PREFIX,
                    })
                connection.execute(sql)

    @classmethod
    def extract_raster_values_to_plots(cls, raster_name):
        with Connector.get_connection() as connection:
            with connection.begin():
                RasterManager.assert_raster_exists(
                    raster_name,
                    connection=connection
                )
                m = "Extracting '{}' raster values to plots properties."
                LOGGER.debug(m.format(raster_name))
                sql = \
                    """
                    WITH {raster_name} AS (
                      SELECT plot.id AS id,
                        ST_Value(raster.rast, plot.location) AS rast_value
                      FROM {plot_table} AS plot
                      LEFT JOIN {raster_table} AS raster
                      ON ST_Intersects(raster.rast, plot.location)
                    )
                    UPDATE {plot_table}
                    SET properties = (
                      {plot_table}.properties || jsonb_build_object(
                        '{prefix}{raster_name}', {raster_name}.rast_value
                      )
                    ) FROM {raster_name}
                    WHERE {raster_name}.id = {plot_table}.id
                    """.format(**{
                        'raster_name': raster_name,
                        'raster_table': '{}.{}'.format(
                            settings.NIAMOTO_RASTER_SCHEMA,
                            raster_name
                        ),
                        'plot_table': '{}.{}'.format(
                            settings.NIAMOTO_SCHEMA,
                            meta.plot.name
                        ),
                        'prefix': RASTER_PROPERTY_PREFIX,
                    })
                connection.execute(sql)
