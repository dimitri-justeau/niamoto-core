# coding: utf-8

from niamoto.data_providers.base_data_provider import BaseDataProvider
from niamoto.data_providers.sql_provider.sql_occurrence_provider import \
    SQLOccurrenceProvider
from niamoto.data_providers.sql_provider.sql_plot_provider import \
    SQLPlotProvider
from niamoto.data_providers.sql_provider.sql_plot_occurrence_provider import \
    SQLPlotOccurrenceProvider


class SQLDataProvider(BaseDataProvider):
    """
    SQL data provider.
    """

    def __init__(self, name, db_url, occurrence_sql=None, plot_sql=None,
                 plot_occurrence_sql=None):
        super(SQLDataProvider, self).__init__(name)
        self.db_url = db_url
        self.occurrence_sql = occurrence_sql
        self.plot_sql = plot_sql
        self.plot_occurrence_sql = plot_occurrence_sql
        self._occurrence_provider = SQLOccurrenceProvider(
            self,
            occurrence_sql
        )
        self._plot_provider = SQLPlotProvider(
            self,
            plot_sql
        )
        self._plot_occurrence_provider = SQLPlotOccurrenceProvider(
            self,
            plot_occurrence_sql
        )

    def sync(self, insert=True, update=True, delete=True,
             sync_occurrence=True, sync_plot=True,
             sync_plot_occurrence=True):
        if self.occurrence_sql is None:
            sync_occurrence = False
        if self.plot_sql is None:
            sync_plot = False
        if self.plot_occurrence_sql is None:
            sync_plot_occurrence = False
        return super(SQLDataProvider, self).sync(
            insert=insert,
            update=update,
            delete=delete,
            sync_occurrence=sync_occurrence,
            sync_plot=sync_plot,
            sync_plot_occurrence=sync_plot_occurrence,
        )

    @classmethod
    def get_type_name(cls):
        raise NotImplementedError()

    @property
    def occurrence_provider(self):
        return self._occurrence_provider

    @property
    def plot_provider(self):
        return self._plot_provider

    @property
    def plot_occurrence_provider(self):
        return self._plot_occurrence_provider
