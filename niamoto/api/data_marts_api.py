# coding: utf-8

"""
Data marts API module
"""

import sqlalchemy as sa

from niamoto.data_marts.fact_tables.fact_table_manager import FactTableManager
from niamoto.data_marts.dimensions.base_dimension import \
    DIMENSION_TYPE_REGISTRY
from niamoto.data_marts.dimensions.dimension_manager import DimensionManager
from niamoto.data_marts.dimensions.vector_dimension import VectorDimension
from niamoto.data_marts.dimensions.taxon_dimension import TaxonDimension
from niamoto.data_marts.dimensions.occurrence_location_dimension import \
    OccurrenceLocationDimension
from niamoto.data_marts.dimensions.vector_hierarchy_dimension import \
    VectorHierarchyDimension
from niamoto.data_marts.dimensions.raster_dimension import RasterDimension
from niamoto.data_marts.dimensional_model import DimensionalModel
from niamoto.api import publish_api
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def get_dimension(dimension_name):
    """
    Return a registered dimension instance.
    :param dimension_name: The dimension name.
    :return: The loaded dimension instance.
    """
    return DimensionManager.get_dimension(dimension_name)


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


def create_raster_dimension(raster_name, cuts=None, value_column_label=None,
                            category_column_label=None, populate=True):
    """
    Create a raster dimension.
    :param raster_name: The raster name.
    :param cuts: Cuts corresponding to categories: ([cuts], [labels]).
        len(labels) = len(cuts) + 1
        e.g: ([10, 20, 30], ['low', 'medium', 'high', 'very high'])
        corresponds to:
            [min_value, 10[   => 'low'
            [10, 20[          => 'medium'
            [20, 30[          => 'high'
            [30, max_value[   => 'very high'
        ]
    :param populate: If True, populate the dimension.
    :param value_column_label: The value column label.
    :param category_column_label: The category column label.
    :return: The created dimension
    """
    dim = RasterDimension(
        raster_name,
        cuts=cuts,
        value_column_label=value_column_label,
        category_column_label=category_column_label,
    )
    dim.create_dimension()
    if populate:
        dim.populate_from_publisher()
    return dim


def create_vector_hierarchy_dimension(name, vector_dimension_names,
                                      populate=True):
    """
    Create a vector hierarchy dimension from registered vector dimensions.
    :param name: The dimension name.
    :param vector_dimension_names:
    :param populate: If True, populate the dimension.
    :return: The created dimension
    """
    dim = VectorHierarchyDimension(
        name,
        [get_dimension(v) for v in vector_dimension_names],
    )
    dim.create_dimension()
    if populate:
        dim.populate_from_publisher()
    return dim


def create_taxon_dimension(dimension_name=TaxonDimension.DEFAULT_NAME,
                           populate=True):
    """
    Create a taxon dimension.
    :param dimension_name: The dimension name.
    :param populate: If True, populate the dimension.
    :return: The created dimension
    """
    dim = TaxonDimension(dimension_name)
    dim.create_dimension()
    if populate:
        dim.populate_from_publisher()
    return dim


def create_occurrence_location_dimension(
        dimension_name=OccurrenceLocationDimension.DEFAULT_NAME,
        populate=True):
    """
    Create an occurrence location dimension.
    :param dimension_name: The dimension name.
    :param populate: If True, populate the dimension.
    :return: The created dimension
    """
    dim = OccurrenceLocationDimension(dimension_name)
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


def delete_dimension(dimension_name):
    """
    Delete a registered dimension.
    :param dimension_name: The name of the dimension to delete.
    """
    return DimensionManager.delete_dimension(dimension_name)


def truncate_dimension(dimension_name):
    """
    Truncate a registered dimension.
    :param dimension_name: The name of the dimension to truncate.
    """
    return DimensionManager.truncate_dimension(dimension_name)


def create_fact_table(name, dimension_names, measure_names,
                      publisher_cls=None):
    """
    Create and register a fact table from existing dimensions.
    :param name: The name of the fact table to create.
    :param dimension_names: An iterable of the fact table's dimension names.
    :param measure_names: An iterable of the fact table's measures names.
    :param publisher_cls: The publisher class to use for populating the
            fact table. Must be a subclass of BaseFactTablePublisher.
    :return: The created fact table object.
    """
    dimensions = [get_dimension(i) for i in dimension_names]
    measure_columns = [sa.Column(i, sa.Float()) for i in measure_names]
    return FactTableManager.register_fact_table(
        name,
        dimensions,
        measure_columns,
        publisher_cls=publisher_cls
    )


def get_fact_table(fact_table_name, publisher_cls=None):
    """
    Load a registered fact table.
    :param fact_table_name: The name of the fact table to load.
    :param publisher_cls: The fact table publisher class.
    :return: The loaded fact table.
    """
    return FactTableManager.get_fact_table(
        fact_table_name,
        publisher_cls=publisher_cls
    )


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


def truncate_fact_table(fact_table_name):
    """
    Truncate a registered fact table.
    :param fact_table_name: The name of the fact table to truncate.
    """
    return FactTableManager.truncate_fact_table(fact_table_name)


def populate_fact_table(fact_table_name, publisher_key, *args, **kwargs):
    """
    Populate a registered fact table using an available publisher.
    :param fact_table_name: The name of the fact table to populate.
    :param publisher_key: The key of the publisher to use for populating the
        fact table.
    """
    fact_table = get_fact_table(
        fact_table_name,
        publisher_cls=publish_api.get_publisher_class(publisher_key)
    )
    fact_table.populate_from_publisher(*args, **kwargs)


def get_dimensional_model(fact_table_name, aggregates):
    """
    Return a DimensionalModel object from a fact table name and a
    dictionary of cubes style aggregates for the fact table.
    :param fact_table_name: The name of the fact table.
    :param aggregates: A list of aggregates.
    :return: A DimensionalModel object constructed from the fact table,
        its dimensions and the passed aggregates.
    """
    fact_table = get_fact_table(fact_table_name)
    dimensions = {dim.name: dim for dim in fact_table.dimensions}
    return DimensionalModel(
        dimensions,
        {fact_table_name: fact_table},
        {fact_table_name: aggregates},
    )
