# coding: utf-8

from sqlalchemy.sql import select, bindparam, and_, cast
from sqlalchemy.dialects.postgresql import JSONB
import pandas as pd

from niamoto.db.metadata import occurrence
from niamoto.taxonomy.taxon import Taxon


class BaseOccurrenceProvider:
    """
    Abstract base class for occurrence provider.
    """

    def __init__(self, data_provider):
        """
        :param data_provider: The parent data provider.
        """
        self.data_provider = data_provider

    def get_niamoto_occurrence_dataframe(self, connection):
        """
        :param connection: A connection to the database to work with.
        :return: A DataFrame containing the occurrence data for this
        provider that is currently stored in the Niamoto database.
        """
        sel = select([occurrence]).where(
            occurrence.c.provider_id == self.data_provider.db_id
        )
        return pd.read_sql(
            sel,
            connection,
            index_col=occurrence.c.id.name,
        )

    def map_provider_taxon_ids(self, dataframe):
        """
        Map provider's taxon ids with Niamoto taxon ids.
        :param dataframe: The dataframe where the mapping has to be done.
        ids. The index must correspond to the provider's pk.
        :return: A series with the same index, the niamoto corresponding
        taxon id as values.
        """
        db = self.data_provider.database
        synonyms = Taxon.get_synonyms_for_provider(
            self.data_provider,
            database=db
        )
        dataframe["provider_taxon_id"] = dataframe["taxon_id"]
        dataframe["taxon_id"] = dataframe["taxon_id"].map(synonyms)

    def get_provider_occurrence_dataframe(self):
        """
        :return: A DataFrame containing the occurrence data currently
        available from the provider. The index of the DataFrame corresponds
        to the provider's pk.
        The dataframe must be structured with the following columns:
            _________________________________________________________________
           | id -> Index of the DataFrame corresponding to the provider's pk |
           |_________________________________________________________________|
           |-----------------------------------------------------------------|
           | taxon_id -> The id of the occurrence's taxon, according to the  |
           |    provider's taxonomic referential (the mapping is done in     |
           |    sync method, if necessary)                                   |
           | ----------------------------------------------------------------|
           | location -> The location of the occurrence (WKT, srid=4326)     |
           |-----------------------------------------------------------------|
           | properties -> The properties of the occurrence, in JSON format  |
            -----------------------------------------------------------------
        """
        raise NotImplementedError()

    def _sync(self, df, connection, insert=True, update=True, delete=True):
        niamoto_df = self.get_niamoto_occurrence_dataframe(connection)
        provider_df = df.where((pd.notnull(df)), None)
        insert_df = self.get_insert_dataframe(niamoto_df, provider_df) \
            if insert else []
        update_df = self.get_update_dataframe(niamoto_df, provider_df) \
            if update else []
        delete_df = self.get_delete_dataframe(niamoto_df, provider_df) \
            if delete else []
        with connection.begin():
            if len(insert_df) > 0:
                ins_stmt = occurrence.insert().values(
                    provider_id=bindparam('provider_id'),
                    provider_pk=bindparam('provider_pk'),
                    location=bindparam('location'),
                    taxon_id=bindparam('taxon_id'),
                    provider_taxon_id=bindparam('provider_taxon_id'),
                    properties=cast(bindparam('properties'), JSONB),
                )
                connection.execute(
                    ins_stmt,
                    insert_df.to_dict(orient='records')
                )
            if len(update_df) > 0:
                upd_stmt = occurrence.update().where(
                    and_(
                        occurrence.c.provider_id == bindparam('prov_id'),
                        occurrence.c.provider_pk == bindparam('prov_pk')
                    )
                ).values({
                    'location': bindparam('location'),
                    'taxon_id': bindparam('taxon_id'),
                    'properties': cast(bindparam('properties'), JSONB),
                    'provider_taxon_id': bindparam('provider_taxon_id'),
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

    def sync(self, connection, insert=True, update=True, delete=True):
        """
        Sync Niamoto database with provider.
        :param connection: A connection to the database to work with.
        :param insert: if False, skip insert operation.
        :param update: if False, skip update operation.
        :param delete: if False, skip delete operation.
        :return: The insert, update, delete DataFrames.
        """
        dataframe = self.get_provider_occurrence_dataframe()
        self.map_provider_taxon_ids(dataframe)
        return self._sync(
            dataframe,
            connection,
            insert=insert,
            update=update,
            delete=delete,
        )

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
        idx = niamoto_dataframe.reset_index().set_index(
            'provider_pk',
        ).loc[diff]['id']
        return niamoto_dataframe.loc[pd.Index(idx)]
