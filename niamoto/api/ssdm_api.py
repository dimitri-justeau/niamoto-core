# coding: utf-8

"""
SSDM API module.
"""

import pandas as pd

from niamoto.ssdm.ssdm_manager import SSDMManager
from niamoto.db.utils import fix_db_sequences
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def get_sdm_list():
    """
    :return: A pandas DataFrame containing all the SDMs available in
    the database.
    """
    return SSDMManager.get_sdm_list()


def add_sdm(taxon_id, raster_file_path, tile_dimension=None,
            register=False, properties={}):
    """
    Add a sdm raster in database and register it in the sdm_registry.
    All SDM rasters are stored in the NIAMOTO_SSDM_SCHEMA schema
    ('niamoto_ssdm' by default').
    :param taxon_id: The id of the corresponding taxon
            (must exist in database)
    :param raster_file_path: The path to the raster file.
    :param tile_dimension: The tile dimension (width, height), if None,
        tile dimension will be chosen automatically by PostGIS.
    :param register: Register the raster as a filesystem (out-db) raster.
        (-R option of raster2pgsql).
    :param properties: A dict of arbitrary properties.
    """
    return SSDMManager.add_sdm(
        taxon_id,
        raster_file_path,
        tile_dimension=tile_dimension,
        register=register,
        properties=properties
    )


def update_sdm(taxon_id, raster_file_path=None, tile_dimension=None,
               register=False, properties=None):
    """
    Update an existing SDM raster.
    """
    return SSDMManager.update_sdm(
        taxon_id,
        raster_file_path=raster_file_path,
        tile_dimension=tile_dimension,
        register=register,
        properties=properties
    )


def delete_sdm(taxon_id):
    """
    Delete an existing SDM raster.
    :param taxon_id: The id of the corresponding taxon
            (must exist in database)
    """
    result = SSDMManager.delete_sdm(taxon_id)
    fix_db_sequences()
    return result


def update_sdm_properties_from_csv(taxon_id, csv_file_path, columns=None):
    """
    Update a SDM properties from a csv file.
    :param taxon_id: The taxon id of the SDM to update.
    :param csv_file_path: The path of the csv containing the properties.
    :param columns: The columns of the csv to use. If None, all the columns
        will be used.
    """
    df = pd.read_csv(csv_file_path)
    if columns is not None:
        df = df[columns]
    props = df.to_dict(orient='record')[0]
    update_sdm(taxon_id, properties=props)
