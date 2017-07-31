# coding: utf-8

import sqlalchemy as sa
from geoalchemy2 import Geometry

from niamoto.data_publishers.occurrence_data_publisher import \
    OccurrenceLocationPublisher
from niamoto.data_marts.dimensions.base_dimension import BaseDimension


class OccurrenceLocationDimension(BaseDimension):
    """
    Dimension representing occurrences location
    """

    DEFAULT_NAME = 'occurrence_location'
    PUBLISHER = OccurrenceLocationPublisher()

    def __init__(self, name=DEFAULT_NAME, publisher=PUBLISHER):
        columns = [
            sa.Column('location', Geometry('POINT', srid=4326))
        ]
        super(OccurrenceLocationDimension, self).__init__(
            name,
            columns,
            publisher=publisher,
            label_col='location',
        )

    def populate(self, dataframe, *args, **kwargs):
        dataframe['location'] = dataframe['location'].apply(
            lambda x: "SRID={};{}".format("4326", x)
        )
        return super(OccurrenceLocationDimension, self).populate(
            dataframe,
            *args,
            **kwargs
        )

    @classmethod
    def get_key(cls):
        return "OCCURRENCE_LOCATION_DIMENSION"

    @classmethod
    def get_description(cls):
        return "Dimension representing occurrences location."

    @classmethod
    def load(cls, dimension_name, label_col='label', properties={}):
        return cls(name=dimension_name)
