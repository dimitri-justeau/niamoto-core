# coding: utf-8

"""
Raster API module.
"""

from niamoto.raster.raster_manager import RasterManager
from niamoto.db.utils import fix_db_sequences
from niamoto.conf import settings


def get_raster_list(database=settings.DEFAULT_DATABASE):
    """
    :param database: The database to work with.
    :return: A pandas DataFrame containing all the raster entries
    available within the given database.
    """
    return RasterManager.get_raster_list(database=database)


def add_raster(raster_file_path, name, tile_width, tile_height,
               srid=None, database=settings.DEFAULT_DATABASE):
    """
    Add a raster in database and register it the Niamoto raster registry.
    Uses raster2pgsql command. The raster is cut in tiles, using the
    dimension tile_width x tile_width. All rasters are stored
    :param raster_file_path: The path to the raster file.
    :param name: The name of the raster.
    :param tile_width: The tile width.
    :param tile_height: The tile height.
    :param srid: SRID to assign to stored raster. If None, use raster's
    metadata to determine which SRID to store.
    :param database: The database to store the raster.
    """
    return RasterManager.add_raster(raster_file_path, name, tile_width,
                                    tile_height, srid=srid, database=database)


def update_raster(raster_file_path, name, tile_width, tile_height,
                  srid=None, database=settings.DEFAULT_DATABASE):
    """
    Update an existing raster in database and register it the Niamoto
    raster registry. Uses raster2pgsql command. The raster is cut in
    tiles, using the dimension tile_width x tile_width. All rasters
    are stored
    :param raster_file_path: The path to the raster file.
    :param name: The name of the raster.
    :param tile_width: The tile width.
    :param tile_height: The tile height.
    :param srid: SRID to assign to stored raster. If None, use raster's
    metadata to determine which SRID to store.
    :param database: The database to store the raster.
    """
    return RasterManager.update_raster(
        raster_file_path, name, tile_width,
        tile_height, srid=srid, database=database
    )


def delete_raster(name, database=settings.DEFAULT_DATABASE):
    """
    Delete an existing raster.
    :param name: The name of the raster.
    :param database: The database to delete the raster from.
    """
    result = RasterManager.delete_raster(name, database=database)
    fix_db_sequences(database=database)
    return result
