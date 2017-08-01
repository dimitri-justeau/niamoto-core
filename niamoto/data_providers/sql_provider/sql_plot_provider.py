# coding: utf-8

import sqlalchemy as sa
import pandas as pd

from niamoto.data_providers.base_plot_provider import BasePlotProvider
from niamoto.exceptions import MalformedDataSourceError


class SQLPlotProvider(BasePlotProvider):
    """
    SQL plot provider. Instantiated with a sql query, that must return
    must contain AT LEAST the following columns:
        id -> The provider's identifier for the plot.
        name -> The name of the plot.
        x -> The longitude of the plot (WGS84).
        y -> The latitude of the plot (WGS84).
    All the remaining column will be stored as properties.
    """

    REQUIRED_COLUMNS = set(['id', 'name', 'x', 'y'])

    def __init__(self, data_provider, plot_sql):
        super(SQLPlotProvider, self).__init__(data_provider)
        self.plot_sql = plot_sql

    def get_provider_plot_dataframe(self):
        connection = sa.create_engine(self.data_provider.db_url).connect()
        df = pd.read_sql(self.plot_sql, connection, index_col='id')
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

