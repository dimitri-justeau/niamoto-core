# coding: utf-8

from niamoto.data_providers.base_plot_occurrence_provider \
    import BasePlotOccurrenceProvider


class CsvPlotOccurrenceProvider(BasePlotOccurrenceProvider):
    """
    Csv plot-occurrence provider.
    """

    def __init__(self, data_provider, plot_occurrence_csv_path):
        super(CsvPlotOccurrenceProvider, self).__init__(data_provider)
        self.plot_occurrence_csv_path = plot_occurrence_csv_path

    def get_provider_plot_occurrence_dataframe(self):
        pass
