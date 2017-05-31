# coding: utf-8

from os.path import exists, isfile

import pandas as pd

from niamoto.data_providers.base_plot_provider import BasePlotProvider
from niamoto.exceptions import DataSourceNotFoundError, \
    MalformedDataSourceError


class CsvPlotProvider(BasePlotProvider):
    """
    Csv plot provider.
    The csv file must contain AT LEAST the following columns:
        id -> The provider's identifier for the plot.
        name -> The name of the plot.
        x -> The longitude of the occurrence (WGS84).
        y -> The latitude of the occurrence (WGS84).
    All the remaining column will be stored as properties.
    """

    REQUIRED_COLUMNS = set(['id', 'name', 'x', 'y'])

    def __init__(self, data_provider, plot_csv_path):
        super(CsvPlotProvider, self).__init__(data_provider)
        if not exists(plot_csv_path) or not isfile(plot_csv_path):
            m = "The plot csv file '{}' does not exist.".format(
                plot_csv_path
            )
            raise DataSourceNotFoundError(m)
        self.plot_csv_path = plot_csv_path

    def get_provider_plot_dataframe(self):
        try:
            df = pd.read_csv(self.plot_csv_path, index_col='id')
        except ValueError:
            m = "The csv file is not valid, it must contains the following " \
                "columns: ('plot_id', 'occurrence_id', " \
                "'occurrence_identifier')"
            raise MalformedDataSourceError(m)
        cols = set(list(df.columns) + ['id', ])
        inter = cols.intersection(self.REQUIRED_COLUMNS)
        if not inter == self.REQUIRED_COLUMNS:
            m = "The csv file does not contains the required columns " \
                "('id', 'taxon_id', 'x', 'y'), csv has: {}".format(cols)
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

