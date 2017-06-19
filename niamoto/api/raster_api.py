# coding: utf-8

"""
Raster API module.
"""

from niamoto.raster.raster_manager import RasterManager
from niamoto.raster.raster_value_extractor import RasterValueExtractor
from niamoto.db.utils import fix_db_sequences
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def get_raster_list():
    """
    :return: A pandas DataFrame containing all the raster entries
    available within the given database.
    """
    return RasterManager.get_raster_list()


def add_raster(raster_file_path, name, tile_dimension=None):
    """
    Add a raster in database and register it the Niamoto raster registry.
    Uses raster2pgsql command. The raster is cut in tiles, using the
    dimension tile_width x tile_width. All rasters are stored
    :param raster_file_path: The path to the raster file.
    :param name: The name of the raster.
    :param tile_dimension: The tile dimension (width, height), if None,
        tile dimension will be chosen automatically by PostGIS.
    """
    return RasterManager.add_raster(
        raster_file_path,
        name,
        tile_dimension=tile_dimension,
    )


def update_raster(raster_file_path, name, new_name=None, tile_dimension=None):
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
    """
    return RasterManager.update_raster(
        raster_file_path,
        name,
        new_name=new_name,
        tile_dimension=tile_dimension,
    )


def delete_raster(name):
    """
    Delete an existing raster.
    :param name: The name of the raster.
    """
    result = RasterManager.delete_raster(name)
    fix_db_sequences()
    return result


def extract_raster_values_to_occurrences(raster_name):
    """
    Extract raster values to occurrences properties.
    :param raster_name: The name of the raster to extract the values from.
    """
    LOGGER.debug("Extracting '{}' raster values to occurrences...".format(
        raster_name
    ))
    RasterValueExtractor.extract_raster_values_to_occurrences(raster_name)


def extract_raster_values_to_plots(raster_name):
    """
    Extract raster values to plots properties.
    :param raster_name: The name of the raster to extract the values from.
    """
    LOGGER.debug("Extracting '{}' raster values to plots...".format(
        raster_name
    ))
    RasterValueExtractor.extract_raster_values_to_plots(raster_name)


def extract_all_rasters_values_to_occurrences():
    """
    Extract raster values to occurrences properties for all registered rasters.
    """
    m1 = "Extracting raster values to occurrences for all registered " \
        "rasters..."
    m2 = "Depending on the number of registered rasters and their " \
        "size, this procedure may take some time."
    LOGGER.debug(m1)
    LOGGER.info(m2)
    for name in get_raster_list()['name']:
        extract_raster_values_to_occurrences(name)


def extract_all_rasters_values_to_plots():
    """
    Extract raster values to plots properties for all registered rasters.
    """
    m1 = "Extracting raster values to plots for all registered " \
        "rasters..."
    m2 = "Depending on the number of registered rasters and their " \
        "size, this procedure may take some time."
    LOGGER.debug(m1)
    LOGGER.info(m2)
    for name in get_raster_list()['name']:
        extract_raster_values_to_plots(name)
