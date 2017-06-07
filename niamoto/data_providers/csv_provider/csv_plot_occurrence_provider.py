# coding: utf-8

from os.path import exists, isfile

import pandas as pd

from niamoto.data_providers.base_plot_occurrence_provider \
    import BasePlotOccurrenceProvider
from niamoto.exceptions import DataSourceNotFoundError, \
    MalformedDataSourceError


class CsvPlotOccurrenceProvider(BasePlotOccurrenceProvider):
    """
    Csv plot-occurrence provider.
    The csv file must contain AT LEAST the following columns:
        plot_id -> The provider's id for the plot.
        occurrence_id -> The provider's id for the occurrence.
        occurrence_identifier -> The occurrence identifier in the plot.
    All the remaining column will be ignored.
    """

    REQUIRED_COLUMNS = set([
        'plot_id',
        'occurrence_id',
        'occurrence_identifier',
    ])

    def __init__(self, data_provider, plot_occurrence_csv_path):
        super(CsvPlotOccurrenceProvider, self).__init__(data_provider)
        self.plot_occurrence_csv_path = plot_occurrence_csv_path

    def get_provider_plot_occurrence_dataframe(self):
        a = exists(self.plot_occurrence_csv_path)
        b = isfile(self.plot_occurrence_csv_path)
        if not a or not b:
            m = "The plot/occurrence csv file '{}' does not exist.".format(
                self.plot_occurrence_csv_path
            )
            raise DataSourceNotFoundError(m)
        try:
            df = pd.read_csv(
                self.plot_occurrence_csv_path,
                index_col=['plot_id', 'occurrence_id']
            )
        except ValueError:
            m = "The csv file is not valid, it must contains the following " \
                "columns: ('plot_id', 'occurrence_id', " \
                "'occurrence_identifier')"
            raise MalformedDataSourceError(m)
        cols = set(list(df.columns) + ['plot_id', 'occurrence_id'])
        inter = cols.intersection(self.REQUIRED_COLUMNS)
        if not inter == self.REQUIRED_COLUMNS:
            m = "The csv file does not contains the required columns " \
                "('plot_id', 'occurrence_id', 'occurrence_identifier')" \
                ", csv has: {}".format(cols)
            raise MalformedDataSourceError(m)
        if len(df) == 0:
            return df
        property_cols = cols.difference(self.REQUIRED_COLUMNS)
        df.drop(property_cols, axis=1, inplace=True)
        return df

