# coding: utf-8

"""
Raster API module.
"""

from niamoto.raster.raster_manager import RasterManager
from niamoto.db.utils import fix_db_sequences


def get_raster_list():
    """
    :return: A pandas DataFrame containing all the raster entries
    available within the given database.
    """
    return RasterManager.get_raster_list()


def add_raster(raster_file_path, name, tile_dimension=None, srid=None):
    """
    Add a raster in database and register it the Niamoto raster registry.
    Uses raster2pgsql command. The raster is cut in tiles, using the
    dimension tile_width x tile_width. All rasters are stored
    :param raster_file_path: The path to the raster file.
    :param name: The name of the raster.
    :param tile_dimension: The tile dimension (width, height), if None,
        tile dimension will be chosen automatically by PostGIS.
    :param srid: SRID to assign to stored raster. If None, use raster's
    metadata to determine which SRID to store.
    """
    return RasterManager.add_raster(
        raster_file_path,
        name,
        tile_dimension=tile_dimension,
        srid=srid
    )


def update_raster(raster_file_path, name, new_name=None, tile_dimension=None,
                  srid=None):
    """
    Update an existing raster in database and register it the Niamoto
    raster registry. Uses raster2pgsql command. The raster is cut in
    tiles, using the dimension tile_width x tile_width. All rasters
    are stored
    :param raster_file_path: The path to the raster file.
    :param name: The name of the raster.
    :param new_name: The new name of the raster (not changed if None).
    :param tile_dimension: The tile dimension (width, height), if None,
        tile dimension will be chosen automatically by PostGIS.
    :param srid: SRID to assign to stored raster. If None, use raster's
    metadata to determine which SRID to store.
    """
    return RasterManager.update_raster(
        raster_file_path,
        name,
        new_name=new_name,
        tile_dimension=tile_dimension,
        srid=srid
    )


def delete_raster(name):
    """
    Delete an existing raster.
    :param name: The name of the raster.
    """
    result = RasterManager.delete_raster(name)
    fix_db_sequences()
    return result
