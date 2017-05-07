# coding: utf-8

from sqlalchemy.sql import select, bindparam, and_
import pandas as pd

from niamoto.db.metadata import occurrence
from niamoto.db.connector import Connector


class BaseOccurrenceProvider:
    """
    Abstract base class for occurrence provider.
    """

    def __init__(self, data_provider):
        """
        :param data_provider: The parent data provider.
        """
        self.data_provider = data_provider

    def get_niamoto_occurrence_dataframe(self):
        """
        :return: A DataFrame containing the occurrence data for this
        provider that is currently stored in the Niamoto database.
        """
        db = self.data_provider.database
        with Connector.get_connection(database=db) as connection:
            sel = select([occurrence]).where(
                occurrence.c.provider_id == self.data_provider.db_id
            )
            return pd.read_sql(
                sel,
                connection,
                index_col=occurrence.c.id.name,
            )

    def map_provider_taxon_ids(self, provider_taxon_ids):
        """
        Map provider's taxon ids with Niamoto taxon ids.
        :param provider_taxon_ids: A series containing the provider's taxon
        ids. The index must correspond to the provider's pk.
        :return: A series with the same index, the niamoto corresponding
        taxon id as values.
        """
        pass  # TODO

    def get_provider_occurrence_dataframe(self):
        """
        :return: A DataFrame containing the occurrence data currently
        available from the provider. The index of the DataFrame corresponds
        to the provider's pk.
        """
        raise NotImplementedError()

    def _sync(self, df):
        niamoto_df = self.get_niamoto_occurrence_dataframe()
        provider_df = df
        insert_df = self.get_insert_dataframe(niamoto_df, provider_df)
        update_df = self.get_update_dataframe(niamoto_df, provider_df)
        delete_df = self.get_delete_dataframe(niamoto_df, provider_df)
        db = self.data_provider.database
        with Connector.get_connection(database=db) as connection:
            with connection.begin():
                if len(insert_df) > 0:
                    ins_stmt = occurrence.insert().values(
                        insert_df.to_dict(orient='records')
                    )
                    connection.execute(ins_stmt)
                if len(update_df) > 0:
                    upd_stmt = occurrence.update().where(
                        and_(
                            occurrence.c.provider_id == bindparam('prov_id'),
                            occurrence.c.provider_pk == bindparam('prov_pk')
                        )
                    ).values({
                        'location': bindparam('location'),
                        'taxon_id': bindparam('taxon_id'),
                        'properties': bindparam('properties'),
                    })
                    connection.execute(
                        upd_stmt,
                        update_df.rename(columns={
                            'provider_id': 'prov_id',
                            'provider_pk': 'prov_pk',
                        }).to_dict(orient='records')
                    )
                if len(delete_df) > 0:
                    del_stmt = occurrence.delete().where(
                        occurrence.c.id.in_(delete_df.index)
                    )
                    connection.execute(del_stmt)
        return insert_df, update_df, delete_df

    def sync(self):
        """
        Sync Niamoto database with provider.
        :return: The insert, update, delete DataFrames.
        """
        return self._sync(self.get_provider_occurrence_dataframe())

    def get_insert_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Occurrence DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Occurrence DataFrame from provider.
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
        :param niamoto_dataframe: Occurrence DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Occurrence DataFrame from provider.
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
        :param niamoto_dataframe: Occurrence DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Occurrence DataFrame from provider.
        :return: The data that is to be deleted to sync Niamoto with the
        provider (i.e. data which is in Niamoto, but not in the provider).
        """
        niamoto_idx = pd.Index(niamoto_dataframe['provider_pk'])
        diff = niamoto_idx.difference(provider_dataframe.index)
        idx = niamoto_dataframe.reset_index(level=0).set_index(
            'provider_pk',
        ).loc[diff]['id']
        return niamoto_dataframe.loc[pd.Index(idx)]
