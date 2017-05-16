# coding: utf-8

from sqlalchemy.sql import *
import pandas as pd

from niamoto.db.metadata import plot_occurrence
from niamoto.db.connector import Connector


class BasePlotOccurrenceProvider:
    """
    Abstract base class for plot-occurrence provider. plot-occurrence data
    corresponds to the association between plots and occurrences.
    """

    def __init__(self, data_provider):
        """
        :param data_provider: The parent data provider.
        """
        self.data_provider = data_provider

    def get_niamoto_plot_occurrence_dataframe(self):
        """
        :return: A DataFrame containing the plot-occurrence data for this
        provider that is currently stored in the Niamoto database.
        """
        db = self.data_provider.database
        with Connector.get_connection(database=db) as connection:
            pass  # TODO

    def get_provider_plot_occurrence_dataframe(self):
        """
        :return: A DataFrame containing the plot-occurrence data currently
        available from the provider. The 'plot_id'  and 'occurrence_id'
        attributes correspond to the provider's pks.
        """
        raise NotImplementedError()

    def _sync(self, df):
        niamoto_df = self.get_niamoto_plot_dataframe()
        provider_df = df
        insert_df = self.get_insert_dataframe(niamoto_df, provider_df)
        update_df = self.get_update_dataframe(niamoto_df, provider_df)
        delete_df = self.get_delete_dataframe(niamoto_df, provider_df)
        db = self.data_provider.database
        with Connector.get_connection(database=db) as connection:
            with connection.begin():
                pass  # TODO
        return insert_df, update_df, delete_df

    def sync(self):
        """
        Sync Niamoto database with provider.
        :return: The insert, update, delete DataFrames.
        """
        return self._sync(self.get_provider_plot_occurrence_dataframe())

    def get_insert_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot-occurrence DataFrame from Niamoto
        database (corresponding to this provider).
        :param provider_dataframe: Plot-occurrence DataFrame from provider.
        :return: The data that is to be inserted to sync Niamoto with the
        provider (i.e. data which is in the provider, but not in Niamoto).
        """
        pass  # TODO

    def get_update_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot-occurrence DataFrame from Niamoto
        database (corresponding to this provider).
        :param provider_dataframe: Plot-occurrence DataFrame from provider.
        :return: The data that is to be updated to sync Niamoto with the
        provider (i.e. data which is both in the provider and Niamoto).
        """
        pass  # TODO

    def get_delete_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot-occurrence DataFrame from Niamoto
        database (corresponding to this provider).
        :param provider_dataframe: Plot-occurrence DataFrame from provider.
        :return: The data that is to be deleted to sync Niamoto with the
        provider (i.e. data which is in Niamoto, but not in the provider).
        """
        pass  # TODO
