# coding: utf-8

import sqlalchemy as sa
import pandas as pd

from niamoto.db import metadata as meta
from niamoto.db.connector import Connector
from niamoto.data_marts.dimensions.base_dimension import \
    DIMENSION_TYPE_REGISTRY


class DimensionManager:
    """
    Dimension manager: register dimensions, list dimensions,
    list dimension types...
    """

    @classmethod
    def get_dimension_types(cls):
        """
        :return: The list of (conformed) dimension types.
        """
        return DIMENSION_TYPE_REGISTRY

    @classmethod
    def get_registered_dimensions(cls):
        """
        :return: The list of registered dimensions.
        """
        sel = sa.select([meta.dimension_registry, ])
        with Connector.get_connection() as connection:
            return pd.read_sql(
                sel,
                connection,
                index_col=meta.dimension_registry.c.id.name
            )

    @classmethod
    def get_dimension(cls, dimension_name):
        """
        Loads a registered dimension.
        :param dimension_name: The name of the dimension to load.
        :return: The loaded dimension.
        """
        sel = sa.select([meta.dimension_registry.c.dimension_key, ]).where(
            meta.dimension_registry.c.name == dimension_name
        )
        with Connector.get_connection() as connection:
            result = connection.execute(sel)
            dim_type = result.fetchone()[0]
        return DIMENSION_TYPE_REGISTRY[dim_type]['class'].load(dimension_name)
