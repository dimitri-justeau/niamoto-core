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
        The index is the multi-index [plot_id, occurrence_id].
        """
        db = self.data_provider.database
        with Connector.get_connection(database=db) as connection:
            sel = select([
                plot_occurrence.c.plot_id,
                plot_occurrence.c.occurrence_id,
                plot_occurrence.c.provider_id,
                plot_occurrence.c.occurrence_identifier,
            ]).where(
                plot_occurrence.c.provider_id == self.data_provider.db_id
            )
            return pd.read_sql(
                sel,
                connection,
                index_col=["plot_id", "occurrence_id"]
            )

    def get_provider_plot_occurrence_dataframe(self):
        """
        :return: A DataFrame containing the plot-occurrence data currently
        available from the provider. The 'plot_id'  and 'occurrence_id'
        attributes correspond to the provider's pks, and must be constitute
        a multi-index [plot_id, occurrence_id].
        """
        raise NotImplementedError()

    def _sync(self, df):
        niamoto_df = self.get_niamoto_plot_occurrence_dataframe()
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

    def get_niamoto_index(self, dataframe):
        """
        :param dataframe: The provider's DataFrame, or a subset, with index
        being a multi-index composed with
        [provider_plot_pk, occurrence_plot_pk].
        :return: The Niamoto's corresponding index:
            provider_plot_pk -> plot_id
            provider_occurrence_pk -> occurrence_id
        """
        pass  # TODO

    def get_insert_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot-occurrence DataFrame from Niamoto
        database (corresponding to this provider). The index is a multi-index
        [plot_id, occurrence_id], where ids correspond to Niamoto ids.
        :param provider_dataframe: Plot-occurrence DataFrame from provider.
        :return: The data that is to be inserted to sync Niamoto with the
        provider (i.e. data which is in the provider, but not in Niamoto).
        """
        niamoto_idx = pd.Index(
            niamoto_dataframe[['provider_plot_pk', 'provider_occurrence_pk']]
        )
        diff = provider_dataframe.index.difference(niamoto_idx)
        df = provider_dataframe.loc[diff]

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
