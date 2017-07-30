# coding: utf-8

import pandas as pd

from niamoto.conf import settings
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.api import data_marts_api
from niamoto.db.connector import Connector
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


class VectorHierarchyPublisher(BaseDataPublisher):
    """
    Publish vector hierarchies from the niamoto vector database.
    Produce an association table containing the identifiers of nested vectors.
    Uses the data from published vector dimensions.
    """

    @classmethod
    def get_key(cls):
        return 'vector_hierarchy'

    @classmethod
    def get_description(cls):
        return "Publish a vector hierarchy from the niamoto vector database."

    @classmethod
    def get_publish_formats(cls):
        return []

    def _process(self, vector_names, *args, buffer_size=0.001, **kwargs):
        """
        :param vector_names: List of the vector names for the hierarchy.
            Ordering is important, the first element corresponds to the
            highest level of the hierarchy while the last element corresponds
            to the smallest level of the hierarchy.
        :return: A GeoDataFrame corresponding to the vector to publish.
        """
        level_ids = ','.join(
            ["{}.id AS {}_id".format(v, v) for v in vector_names]
        )
        where_clause = "WHERE " + " AND ".join(
            ["{}.id IS NOT NULL".format(v) for v in vector_names]
        )
        highest_level = vector_names.pop(0)
        dim_tables = "{schema}.{tb} AS {tb}".format(**{
            'schema': settings.NIAMOTO_DIMENSIONS_SCHEMA,
            'tb': highest_level,
        })
        previous_level = highest_level
        previous_geom = data_marts_api.get_dimension(
            highest_level
        ).geom_col[0]
        for level in vector_names:
            geom = data_marts_api.get_dimension(level).geom_col[0]
            dim_tables += \
                """
                LEFT JOIN {schema}.{tb} AS {tb}
                    ON ST_Within(
                        ST_Buffer({tb}.{geom}, -{buffer}),
                        {prev_tb}.{prev_geom}
                    )
                """.format(**{
                    'schema': settings.NIAMOTO_DIMENSIONS_SCHEMA,
                    'tb': level,
                    'prev_tb': previous_level,
                    'geom': geom,
                    'prev_geom': previous_geom,
                    'buffer': buffer_size,
                })
            previous_level = level
            previous_geom = geom
        sql = \
            """
            SELECT {level_ids}
            FROM {dim_tables}
            {where_clause};
            """.format(
                **{
                    'level_ids': level_ids,
                    'dim_tables': dim_tables,
                    'where_clause': where_clause,
                }
            )
        with Connector.get_connection() as connection:
            df = pd.read_sql(sql, connection)
            return df
