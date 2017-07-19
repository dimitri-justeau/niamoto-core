# coding: utf-8

"""
Data marts API module
"""

from niamoto.data_marts.dimensions.dimension_manager import DimensionManager
from niamoto.data_marts.dimensions.vector_dimension import VectorDimension
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def create_vector_dimension(vector_name, populate=True):
    """
    Create a vector dimension from a registered vector.
    :param vector_name: The vector name.
    :param populate: If True, populate the dimension.
    :return: The created dimension.
    """
    dim = VectorDimension(vector_name)
    dim.create_dimension()
    if populate:
        dim.populate_from_publisher()
    return dim


def get_dimension(dimension_name):
    return DimensionManager.get_dimension(dimension_name)
