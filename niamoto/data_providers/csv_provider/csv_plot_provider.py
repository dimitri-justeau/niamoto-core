# coding: utf-8

from os.path import exists, isfile

from niamoto.data_providers.base_plot_provider import BasePlotProvider
from niamoto.exceptions import DataSourceNotFoundError, \
    MalformedDataSourceError


class CsvPlotProvider(BasePlotProvider):
    """
    Csv plot provider.
    """

    def __init__(self, data_provider, plot_csv_path):
        super(CsvPlotProvider, self).__init__(data_provider)
        if not exists(plot_csv_path) or not isfile(plot_csv_path):
            m = "The plot csv file '{}' does not exist.".format(
                plot_csv_path
            )
            raise DataSourceNotFoundError(m)
        self.plot_csv_path = plot_csv_path

    def get_provider_plot_dataframe(self):
        pass

