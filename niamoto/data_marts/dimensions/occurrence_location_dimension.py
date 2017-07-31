# coding: utf-8

import sqlalchemy as sa
from geoalchemy2 import Geometry
import geopandas as gpd

from niamoto.conf import settings
from niamoto.db.connector import Connector
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
            sa.Column('location', Geometry('POINT', srid=4326)),
            sa.Column('location_wkt', sa.String())
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

    def get_values(self, wkt_filter=None):
        where_clause = "WHERE location IS NOT NULL"
        if wkt_filter is not None:
            where_clause += \
                " AND ST_Intersects(location, " \
                "ST_GeomFromEWKT('SRID=4326;{}'))".format(
                    wkt_filter
                )
        sql = "SELECT * FROM {}.{} {};".format(
            settings.NIAMOTO_DIMENSIONS_SCHEMA,
            self.name,
            where_clause
        )
        with Connector.get_connection() as connection:
            df = gpd.read_postgis(
                sql,
                connection,
                index_col='id',
                geom_col='location',
            )
        return df
