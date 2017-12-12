# coding: utf-8

"""
Vector API module.
"""

from niamoto.vector.vector_manager import VectorManager
from niamoto.db.utils import fix_db_sequences
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def get_vector_list():
    """
    :return: A pandas DataFrame containing all the vector available in the
    database.
    """
    return VectorManager.get_vector_list()


def add_vector(name, vector_file_path):
    """
    Add a vector in database and register it the Niamoto vector registry.
    Uses ogr2ogr. All vectors are stored in the
    settings.NIAMOTO_RASTER_SCHEMA schema.
    :param name: The name of the vector. The created table will have this
    name.
    :param vector_file_path: The path to the vector file.
    """
    return VectorManager.add_vector(
        name,
        vector_file_path,
    )


def update_vector(name, vector_file_path=None, new_name=None,
                  properties=None):
    """
    Update an existing vector in database and update it the Niamoto
    vector registry. Uses ogr2ogr. All vectors are stored in the
    settings.NIAMOTO_RASTER_SCHEMA schema.
    :param name: The name of the vector.
    :param vector_file_path: The path to the vector file. If None, the vector
        data won't be updated.
    :param new_name: The new name of the vector (not changed if None).
    :param properties: A dict of arbitrary properties.
    """
    return VectorManager.update_vector(
        name,
        vector_file_path,
        new_name=new_name,
        properties=properties
    )


def delete_vector(name):
    """
    Delete an existing vector.
    :param name: The name of the vector.
    """
    result = VectorManager.delete_vector(name)
    fix_db_sequences()
    return result
