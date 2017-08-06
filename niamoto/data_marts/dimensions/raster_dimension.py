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

    def __init__(self, raster_name, cuts=None, value_column_label=None,
                 category_column_label=None):
        """
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
        :param value_column_label: The value column label.
        :param category_column_label: The category column label.
        """
        self.raster_name = raster_name
        self.cuts = cuts
        column_labels = {}
        if value_column_label is not None:
            column_labels[self.raster_name] = value_column_label
        if category_column_label is not None:
            column_labels['category'] = category_column_label
        properties = {}
        columns = [
            sa.Column(self.raster_name, sa.Float),
            sa.Column("pixel_count", sa.Integer)
        ]
        if self.cuts is not None:
            assert len(cuts[1]) == len(cuts[0]) + 1
            columns += [
                sa.Column("category", sa.String)
            ]
            properties['cuts'] = cuts
        super(RasterDimension, self).__init__(
            raster_name,
            columns,
            publisher=RasterValueCountPublisher(),
            label_col=self.raster_name,
            properties=properties,
            column_labels=column_labels
        )

    @classmethod
    def load(cls, dimension_name, label_col='label', properties={},
             column_labels={}):
        cuts = None
        if 'cuts' in properties:
            cuts = properties['cuts']
        val_col_label = None
        cat_col_label = None
        if 'column_labels' in properties:
            val_col_label = properties['column_labels'].get(
                dimension_name,
                None
            )
            cat_col_label = properties['column_labels'].get('category', None)
        return cls(
            dimension_name,
            cuts=cuts,
            value_column_label=val_col_label,
            category_column_label=cat_col_label,
        )

    def populate_from_publisher(self, *args, **kwargs):
        return super(RasterDimension, self).populate_from_publisher(
            self.raster_name,
            *args,
            cuts=self.cuts,
            **kwargs
        )

    @classmethod
    def get_description(cls):
        return "Raster dimension, values are extracted from " \
               "a registered raster."

    @classmethod
    def get_key(cls):
        return "RASTER_DIMENSION"

    def get_cubes_levels(self):
        if self.cuts is None:
            return super(RasterDimension, self).get_cubes_levels()
        levels = [
            {
                'name': 'category',
                'attributes': [{
                    'name': 'category',
                    'label': self.get_column_labels().get(
                        'category',
                        'category'
                    )
                },
                ]
            },
            {
                'name': self.name,
                'attributes': [
                    {
                        'name': 'id',
                        'label': self.get_column_labels().get('id', 'id')
                    },
                    {
                        'name': self.name,
                        'label': self.get_column_labels().get(
                            self.name,
                            self.name
                        )
                    },
                ],
            }
        ]
        return levels
