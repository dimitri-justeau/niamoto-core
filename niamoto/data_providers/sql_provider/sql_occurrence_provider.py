# coding: utf-8

import sqlalchemy as sa
import pandas as pd

from niamoto.data_providers.base_occurrence_provider import \
    BaseOccurrenceProvider
from niamoto.exceptions import MalformedDataSourceError


class SQLOccurrenceProvider(BaseOccurrenceProvider):
    """
    SQL occurrence provider. Instantiated with a sql query, that must return
    AT LEAST the following columns:
        id -> The provider's identifier for the occurrence.
        taxon_id -> The provider's taxon id for the occurrence.
        x -> The longitude of the occurrence (WGS84).
        y -> The latitude of the occurrence (WGS84).
    All the remaining column will be stored as properties.
    """

    REQUIRED_COLUMNS = set(['id', 'taxon_id', 'x', 'y'])

    def __init__(self, data_provider, occurrence_sql):
        super(SQLOccurrenceProvider, self).__init__(data_provider)
        self.occurrence_sql = occurrence_sql

    def get_provider_occurrence_dataframe(self):
        connection = sa.create_engine(self.data_provider.db_url).connect()
        df = pd.read_sql(self.occurrence_sql, connection, index_col='id')
        cols = set(list(df.columns) + ['id', ])
        inter = cols.intersection(self.REQUIRED_COLUMNS)
        if not inter == self.REQUIRED_COLUMNS:
            m = "The queried data does not contains the required columns " \
                "('id', 'taxon_id', 'x', 'y'), " \
                "queried data has: {}".format(cols)
            raise MalformedDataSourceError(m)
        if len(df) == 0:
            return df
        property_cols = cols.difference(self.REQUIRED_COLUMNS)
        if len(property_cols) > 0:
            properties = df[list(property_cols)].apply(
                lambda x: x.to_json(),
                axis=1
            )
        else:
            properties = '{}'
        df.drop(property_cols, axis=1, inplace=True)
        df['properties'] = properties
        location = df[['x', 'y']].apply(
            lambda x: "SRID=4326;POINT({} {})".format(x['x'], x['y']),
            axis=1
        )
        df['location'] = location
        df.drop(['x', 'y'], axis=1, inplace=True)
        return df
