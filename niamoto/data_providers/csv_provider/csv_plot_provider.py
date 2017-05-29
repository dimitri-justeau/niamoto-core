# coding: utf-8

from niamoto.data_providers.base_plot_provider import BasePlotProvider


class CsvPlotProvider(BasePlotProvider):
    """
    Csv plot provider.
    """

    def __init__(self, data_provider, plot_csv_path):
        super(CsvPlotProvider, self).__init__(data_provider)
        self.plot_csv_path = plot_csv_path

    def get_provider_plot_dataframe(self):
        pass

