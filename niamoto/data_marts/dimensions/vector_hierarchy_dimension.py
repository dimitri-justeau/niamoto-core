# coding: utf-8

import sqlalchemy as sa

from niamoto.conf import settings
from niamoto.data_marts.dimensions.base_dimension import BaseDimension


class VectorHierarchyDimension(BaseDimension):
    """
    Vector hierarchy dimension: Represent a hierarchy of vectors containing
    each others. The vector hierarchy dimension is implemented as a snowflaked
    dimension build from existing vector dimensions.
    """

    def __init__(self, name, vector_dimensions):
        self.vector_dimensions = vector_dimensions
        columns = []
        for vector_dim in self.vector_dimensions:
            col_name = "{}_{}".format(vector_dim.name, vector_dim.pk.name)
            columns.append(
                sa.Column(
                    col_name,
                    sa.ForeignKey(
                        "{}.{}.{}".format(
                            settings.NIAMOTO_DIMENSIONS_SCHEMA,
                            vector_dim.name,
                            vector_dim.pk.name
                        ),
                        ondelete='CASCADE',
                        onupdate='CASCADE',
                    ),
                    index=True,
                )
            )
        super(VectorHierarchyDimension, self).__init__(
            name,
            columns,
            publisher=None,  # TODO
            label_col=None,  # TODO
        )

    @classmethod
    def get_key(cls):
        return "VECTOR_HIERARCHY_DIMENSION"

    @classmethod
    def get_description(cls):
        return "Vector hierarchy dimension."
