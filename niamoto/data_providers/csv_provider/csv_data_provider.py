# coding: utf-8

from niamoto.data_providers.base_data_provider import BaseDataProvider
from niamoto.data_providers.csv_provider.csv_occurrence_provider \
    import CsvOccurrenceProvider
from niamoto.data_providers.csv_provider.csv_plot_provider \
    import CsvPlotProvider
from niamoto.data_providers.csv_provider.csv_plot_occurrence_provider \
    import CsvPlotOccurrenceProvider


class CsvDataProvider(BaseDataProvider):
    """
    Csv file data provider.
    """

    def __init__(self, name, occurrence_csv_path=None,
                 plot_csv_path=None, plot_occurrence_csv_path=None):
        super(CsvDataProvider, self).__init__(name)
        self.occurrence_csv_path = occurrence_csv_path
        self.plot_csv_path = plot_csv_path
        self.plot_occurrence_csv_path = plot_occurrence_csv_path
        self._occurrence_provider = CsvOccurrenceProvider(
            self,
            occurrence_csv_path
        )
        self._plot_provider = CsvPlotProvider(
            self,
            plot_csv_path
        )
        self._plot_occurrence_provider = CsvPlotOccurrenceProvider(
            self,
            plot_occurrence_csv_path
        )

    def sync(self, insert=True, update=True, delete=True,
             sync_occurrence=True, sync_plot=True,
             sync_plot_occurrence=True):
        if self.occurrence_csv_path is None:
            sync_occurrence = False
        if self.plot_csv_path is None:
            sync_plot = False
        if self.plot_occurrence_csv_path is None:
            sync_plot_occurrence = False
        return super(CsvDataProvider, self).sync(
            insert=insert,
            update=update,
            delete=delete,
            sync_occurrence=sync_occurrence,
            sync_plot=sync_plot,
            sync_plot_occurrence=sync_plot_occurrence,
        )

    @property
    def occurrence_provider(self):
        return self._occurrence_provider

    @property
    def plot_provider(self):
        return self._plot_provider

    @property
    def plot_occurrence_provider(self):
        return self._plot_occurrence_provider

    @classmethod
    def get_type_name(cls):
        return "CSV"
