# coding: utf-8

from sqlalchemy.sql import *
import pandas as pd

from niamoto.db.metadata import plot_occurrence, plot, occurrence
from niamoto.db.connector import Connector


class BasePlotOccurrenceProvider:
    """
    Abstract base class for plot-occurrence provider. plot-occurrence data
    corresponds to the association between plots and occurrences.
    /!\/!\ The plot-occurrence provider sync method must be used after having
    synced plots and occurrences with the plot provider and the occurrence
    provider.
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
                plot_occurrence.c.provider_plot_pk,
                plot_occurrence.c.provider_occurrence_pk,
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
        available from the provider. The 'provider_plot_id' and
        'provider_occurrence_id' attributes correspond to the provider's pks,
        and must be constitute a multi-index [provider_plot_id,
        provider_occurrence_id]. The structure must be the following:
            _________________________________________________________________
           | provider_plot_id       -> First member of the multi index       |
           | provider_occurrence_id -> Second member of the multi index      |
           |_________________________________________________________________|
           |-----------------------------------------------------------------|
           | occurrence_identifier -> The identifier of the occurrence in    |
           |    the plot                                                     |
            -----------------------------------------------------------------
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

    def get_reindexed_provider_dataframe(self, dataframe):
        """
        :param dataframe: The provider's DataFrame, or a subset, with index
        being a multi-index composed with
        [provider_plot_pk, occurrence_plot_pk].
        :return: The dataframe reindexed:
            provider_plot_pk -> plot_id
            provider_occurrence_pk -> occurrence_id
        """
        if len(dataframe) == 0:
            return dataframe
        db = self.data_provider.database
        with Connector.get_connection(database=db) as connection:
            sel_plot = select([
                plot.c.id.label('plot_id'),
                plot.c.provider_pk.label('provider_plot_pk')
            ]).where(plot.c.provider_id == self.data_provider.db_id)
            sel_occ = select([
                occurrence.c.id.label('occurrence_id'),
                occurrence.c.provider_pk.label('provider_occurrence_pk')
            ]).where(occurrence.c.provider_id == self.data_provider.db_id)
            plot_ids = pd.read_sql(
                sel_plot,
                connection,
                index_col='provider_plot_pk'
            )
            occ_ids = pd.read_sql(
                sel_occ,
                connection,
                index_col='provider_occurrence_pk'
            )
        plot_id = plot_ids
        occ_id = occ_ids
        dataframe.reset_index(inplace=True)
        dataframe = dataframe.merge(
            plot_id,
            left_on='provider_plot_pk',
            right_index=True,
        )
        dataframe = dataframe.merge(
            occ_id,
            left_on='provider_occurrence_pk',
            right_index=True,
        )
        dataframe.set_index(['plot_id', 'occurrence_id'], inplace=True)
        return dataframe

    def get_insert_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot-occurrence DataFrame from Niamoto
        database (corresponding to this provider). The index is a multi-index
        [plot_id, occurrence_id], where ids correspond to Niamoto ids.
        :param provider_dataframe: Plot-occurrence DataFrame from provider.
        Must have been reindexed with the get_reindexed_provider_dataframe
        method.
        :return: The data that is to be inserted to sync Niamoto with the
        provider (i.e. data which is in the provider, but not in Niamoto).
        """
        if len(provider_dataframe) == 0:
            return provider_dataframe
        diff = provider_dataframe.index.difference(niamoto_dataframe.index)
        return provider_dataframe.loc[diff]

    def get_update_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot-occurrence DataFrame from Niamoto
        database (corresponding to this provider).
        :param provider_dataframe: Plot-occurrence DataFrame from provider.
        :return: The data that is to be updated to sync Niamoto with the
        provider (i.e. data which is both in the provider and Niamoto).
        """
        if len(provider_dataframe) == 0:
            return provider_dataframe
        inter = provider_dataframe.index.intersection(niamoto_dataframe.index)
        return provider_dataframe.loc[inter]

    def get_delete_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot-occurrence DataFrame from Niamoto
        database (corresponding to this provider).
        :param provider_dataframe: Plot-occurrence DataFrame from provider.
        :return: The data that is to be deleted to sync Niamoto with the
        provider (i.e. data which is in Niamoto, but not in the provider).
        """
        if len(provider_dataframe) == 0:
            return niamoto_dataframe
        diff = niamoto_dataframe.index.difference(provider_dataframe.index)
        return niamoto_dataframe.loc[diff]
