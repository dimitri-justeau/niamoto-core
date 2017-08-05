# coding: utf-8

import sqlalchemy as sa

from niamoto.data_marts.dimensions.base_dimension import BaseDimension
from niamoto.data_publishers.raster_data_publisher import \
    RasterValueCountPublisher


class RasterDimension(BaseDimension):
    """
    Dimension extracted from a registered raster, contains the different
    values of a raster and their associated pixel count.
    """

    def __init__(self, raster_name):
        self.raster_name = raster_name
        columns = [
            sa.Column(self.raster_name, sa.Float),
            sa.Column("pixel_count", sa.Integer)
        ]
        super(RasterDimension, self).__init__(
            raster_name,
            columns,
            publisher=RasterValueCountPublisher(),
            label_col=self.raster_name,
        )

    @classmethod
    def load(cls, dimension_name, label_col='label', properties={}):
        return cls(dimension_name)

    def populate_from_publisher(self, *args, **kwargs):
        return super(RasterDimension, self).populate_from_publisher(
            self.raster_name,
            *args,
            **kwargs
        )

    @classmethod
    def get_description(cls):
        return "Raster dimension, values are extracted from " \
               "a registered raster."

    @classmethod
    def get_key(cls):
        return "RASTER_DIMENSION"
