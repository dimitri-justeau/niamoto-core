# coding: utf-8

from sqlalchemy.sql import select
import pandas as pd

from niamoto.db.metadata import plot
from niamoto.db.connector import Connector
from niamoto.settings import DEFAULT_DATABASE, NIAMOTO_SCHEMA


class BasePlotProvider:
    """
    Abstract base class for plot provider.
    """

    def __init__(self, data_provider):
        """
        :param data_provider: The parent data provider.
        """
        self.data_provider = data_provider

    def get_niamoto_plot_dataframe(self):
        """
        :return: A DataFrame containing the plot data for this
        provider that is currently stored in the Niamoto database.
        """
        db = self.data_provider.database
        with Connector.get_connection(database=db) as connection:
            sel = select([plot]).where(
                plot.c.provider_id == self.data_provider.db_id
            )
            return pd.read_sql(
                sel,
                connection,
                index_col=plot.c.id.name,
            )

    def get_provider_occurrence_dataframe(self):
        """
        :return: A DataFrame containing the plot data currently
        available from the provider. The index of the DataFrame corresponds
        to the provider's pk.
        """
        raise NotImplementedError()

    def get_insert_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Plot DataFrame from provider.
        :return: The data that is to be inserted to sync Niamoto with the
        provider (i.e. data which is in the provider, but not in Niamoto).
        """
        niamoto_idx = pd.Index(niamoto_dataframe['provider_pk'])
        diff = provider_dataframe.index.difference(niamoto_idx)
        df = provider_dataframe.loc[diff]
        df['provider_pk'] = df.index
        df['provider_id'] = self.data_provider.db_id
        return df

    def get_update_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Plot DataFrame from provider.
        :return: The data that is to be updated to sync Niamoto with the
        provider (i.e. data which is both in the provider and Niamoto).
        """
        niamoto_idx = pd.Index(niamoto_dataframe['provider_pk'])
        inter = provider_dataframe.index.intersection(niamoto_idx)
        df = provider_dataframe.loc[inter]
        df['provider_pk'] = df.index
        df['provider_id'] = self.data_provider.db_id
        return df

    def get_delete_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Plot DataFrame from provider.
        :return: The data that is to be deleted to sync Niamoto with the
        provider (i.e. data which is in Niamoto, but not in the provider).
        """
        niamoto_idx = pd.Index(niamoto_dataframe['provider_pk'])
        diff = niamoto_idx.difference(provider_dataframe.index)
        idx = niamoto_dataframe.reset_index(level=0).set_index(
            'provider_pk',
        ).loc[diff]['id']
        return niamoto_dataframe.loc[pd.Index(idx)]
