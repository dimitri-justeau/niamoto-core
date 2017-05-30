# coding: utf-8

from os.path import exists, isfile

import pandas as pd

from niamoto.data_providers.base_plot_provider import BasePlotProvider
from niamoto.exceptions import DataSourceNotFoundError, \
    MalformedDataSourceError


class CsvPlotProvider(BasePlotProvider):
    """
    Csv plot provider.
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
        df = pd.read_csv(self.plot_csv_path)
        cols = set(df.columns)
        inter = cols.intersection(self.REQUIRED_COLUMNS)
        if not inter == self.REQUIRED_COLUMNS:
            m = "The csv file does not contains the required columns " \
                "('id', 'taxon_id', 'x', 'y'), csv has: {}".format(cols)
            raise MalformedDataSourceError(m)
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

