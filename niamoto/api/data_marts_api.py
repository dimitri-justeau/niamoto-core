# coding: utf-8

"""
Data marts API module
"""

from niamoto.data_marts.fact_tables.fact_table_manager import FactTableManager
from niamoto.data_marts.dimensions.base_dimension import \
    DIMENSION_TYPE_REGISTRY
from niamoto.data_marts.dimensions.dimension_manager import DimensionManager
from niamoto.data_marts.dimensions.vector_dimension import VectorDimension
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def create_vector_dimension(vector_name, label_col='label', populate=True):
    """
    Create a vector dimension from a registered vector.
    :param vector_name: The vector name.
    :param label_col: The label column name of the dimension.
    :param populate: If True, populate the dimension.
    :return: The created dimension.
    """
    dim = VectorDimension(vector_name, label_col)
    dim.create_dimension()
    if populate:
        dim.populate_from_publisher()
    return dim


def get_registered_dimensions():
    """
    :return: The registered dimensions.
    """
    return DimensionManager.get_registered_dimensions()


def get_dimension_types():
    """
    :return: The available dimension types.
    """
    return DIMENSION_TYPE_REGISTRY


def get_dimension(dimension_name):
    """
    Return a registered dimension instance.
    :param dimension_name: The dimension name.
    :return: The loaded dimension instance.
    """
    return DimensionManager.get_dimension(dimension_name)


def delete_dimension(dimension_name):
    """
    Delete a registered dimension.
    :param dimension_name: The name of the dimension to delete.
    """
    return DimensionManager.delete_dimension(dimension_name)


def get_registered_fact_tables():
    """
    :return: The registered fact tables.
    """
    return FactTableManager.get_registered_fact_tables()


def delete_fact_table(fact_table_name):
    """
    Delete a registered fact table.
    :param fact_table_name: The name of the fact table to delete.
    """
    return FactTableManager.delete_fact_table(fact_table_name)
