# coding: utf-8

from os.path import exists, isfile

from niamoto.data_providers.base_plot_occurrence_provider \
    import BasePlotOccurrenceProvider
from niamoto.exceptions import DataSourceNotFoundError, \
    MalformedDataSourceError


class CsvPlotOccurrenceProvider(BasePlotOccurrenceProvider):
    """
    Csv plot-occurrence provider.
    """

    def __init__(self, data_provider, plot_occurrence_csv_path):
        super(CsvPlotOccurrenceProvider, self).__init__(data_provider)
        a = exists(plot_occurrence_csv_path)
        b = isfile(plot_occurrence_csv_path)
        if not a or not b:
            m = "The plot/occurrence csv file '{}' does not exist.".format(
                plot_occurrence_csv_path
            )
            raise DataSourceNotFoundError(m)
        self.plot_occurrence_csv_path = plot_occurrence_csv_path

    def get_provider_plot_occurrence_dataframe(self):
        pass
