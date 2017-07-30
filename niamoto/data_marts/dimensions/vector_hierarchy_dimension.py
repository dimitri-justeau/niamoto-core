# coding: utf-8

import sqlalchemy as sa

from niamoto.conf import settings
from niamoto.data_publishers.vector_hierarchy_publisher import \
    VectorHierarchyPublisher
from niamoto.data_marts.dimensions.base_dimension import BaseDimension
from niamoto.data_marts.dimensions.vector_dimension import VectorDimension


class VectorHierarchyDimension(BaseDimension):
    """
    Vector hierarchy dimension: Represent a hierarchy of vectors containing
    each others. The vector hierarchy dimension is implemented as a snowflaked
    dimension build from existing vector dimensions.
    """

    def __init__(self, name, vector_dimensions, label_col='label'):
        self.vector_dimensions = vector_dimensions
        columns = []
        self.levels = []
        for vector_dim in self.vector_dimensions:
            self.levels.append(vector_dim.name)
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
            publisher=VectorHierarchyPublisher(),
            label_col=label_col,
            properties={
                'levels': self.levels
            }
        )

    @classmethod
    def get_key(cls):
        return "VECTOR_HIERARCHY_DIMENSION"

    @classmethod
    def get_description(cls):
        return "Vector hierarchy dimension."

    @classmethod
    def load(cls, dimension_name, label_col='label', properties={}):
        if 'levels' not in properties:
            raise ValueError(
                'The VectorHierarchyDimension needs a "levels" property to be'
                ' loaded from the database.'
            )
        levels = properties['levels']
        dims = [VectorDimension(level) for level in levels]
        return cls(
            dimension_name,
            dims,
            label_col,
        )

    def populate_from_publisher(self, *args, **kwargs):
        return super(VectorHierarchyDimension, self).populate_from_publisher(
            self.levels,
            *args,
            **kwargs,
        )
