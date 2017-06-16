# coding: utf-8

from sqlalchemy import Table, func

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
            RasterManager.assert_raster_exists(
                raster_name,
                connection=connection
            )
            LOGGER.debug(
                "Extracting '{}' raster values in occurrences.".format(
                    raster_name
                )
            )
            raster_table = Table(
                raster_name,
                meta.metadata,
                schema=settings.NIAMOTO_RASTER_SCHEMA,
                autoload=True,
                autoload_with=connection,
            )
            upd = meta.occurrence.update().where(
                func.st_intersects(
                    raster_table.c.rast,
                    meta.occurrence.c.location
                )
            ).values({
                'properties': meta.occurrence.c.properties.concat(
                    func.jsonb_build_object(
                        raster_name,
                        func.st_value(
                            raster_table.c.rast,
                            meta.occurrence.c.location
                        )
                    )
                )
            })
            import time
            t = time.time()
            connection.execute(upd)
            print(time.time() - t)


