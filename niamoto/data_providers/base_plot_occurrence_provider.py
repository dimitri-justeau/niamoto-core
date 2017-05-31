# coding: utf-8

from sqlalchemy.sql import and_, bindparam, select
import pandas as pd

from niamoto.db.metadata import plot_occurrence, plot, occurrence
from niamoto.db.connector import Connector
from niamoto.exceptions import IncoherentDatabaseStateError


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

    def get_niamoto_plot_occurrence_dataframe(self, connection):
        """
        :param connection: A connection to the database to work with.
        :return: A DataFrame containing the plot-occurrence data for this
        provider that is currently stored in the Niamoto database.
        The index is the multi-index [plot_id, occurrence_id].
        """
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
        available from the provider. The 'provider_plot_pk' and
        'provider_occurrence_pk' attributes correspond to the provider's pks,
        and must be constitute a multi-index [provider_plot_pk,
        provider_occurrence_pk]. The structure must be the following:
            _________________________________________________________________
           | plot_id       -> 1st member of the multi index, provider's id   |
           | occurrence_id -> 2nd member of the multi index, provider's id   |
           |_________________________________________________________________|
           |-----------------------------------------------------------------|
           | occurrence_identifier -> The identifier of the occurrence in    |
           |    the plot                                                     |
            -----------------------------------------------------------------
        """
        raise NotImplementedError()

    def _sync(self, df, connection, insert=True, update=True, delete=True):
        niamoto_df = self.get_niamoto_plot_occurrence_dataframe(connection)
        provider_df = df
        insert_df = self.get_insert_dataframe(niamoto_df, provider_df) \
            if insert else pd.DataFrame()
        update_df = self.get_update_dataframe(niamoto_df, provider_df) \
            if update else pd.DataFrame()
        delete_df = self.get_delete_dataframe(niamoto_df, provider_df) \
            if delete else pd.DataFrame()
        with connection.begin():
            plot_id_col = plot_occurrence.c.plot_id
            occurrence_id_col = plot_occurrence.c.occurrence_id
            if len(insert_df) > 0:
                ins_stmt = plot_occurrence.insert().values(
                    insert_df.to_dict(orient='records')
                )
                connection.execute(ins_stmt)
            if len(update_df) > 0:
                upd_stmt = plot_occurrence.update().where(
                    and_(
                        plot_id_col == bindparam('_plot_id'),
                        occurrence_id_col == bindparam('_occurrence_id')
                    )
                ).values({
                    'occurrence_identifier': bindparam(
                        'occurrence_identifier'
                    )
                })
                connection.execute(
                    upd_stmt,
                    update_df.rename(columns={
                        'plot_id': '_plot_id',
                        'occurrence_id': '_occurrence_id',
                    }).to_dict(orient='records')
                )
            if len(delete_df) > 0:
                del_stmt = plot_occurrence.delete().where(
                    and_(
                        plot_id_col == bindparam('plot_id'),
                        occurrence_id_col == bindparam('occurrence_id')
                    )
                )
                connection.execute(
                    del_stmt,
                    delete_df[[
                        'plot_id',
                        'occurrence_id',
                    ]].to_dict(orient='records')
                )
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
        df = self.get_provider_plot_occurrence_dataframe()
        reindexed_df = self.get_reindexed_provider_dataframe(df)
        return self._sync(
            reindexed_df,
            connection,
            insert=insert,
            update=update,
            delete=delete,
        )

    def get_reindexed_provider_dataframe(self, dataframe):
        """
        :param dataframe: The provider's DataFrame, or a subset, with index
        being a multi-index composed with
        [plot_id, occurrence_id] (provider's ids).
        :return: The dataframe reindexed:
            provider_plot_pk -> plot_id
            provider_occurrence_pk -> occurrence_id
        """

        if len(dataframe) == 0:
            return dataframe
        # Set index names
        dataframe.index.set_names(
            ['provider_plot_pk', 'provider_occurrence_pk'],
            inplace=True
        )
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
        dataframe.reset_index(inplace=True)
        # Assert plots and occurrences exist in db
        plot_pks = pd.Index(pd.unique(dataframe['provider_plot_pk']))
        plot_not_in_db = plot_pks.difference(plot_ids.index)
        occ_pks = pd.Index(pd.unique(dataframe['provider_occurrence_pk']))
        occ_not_in_db = occ_pks.difference(occ_ids.index)
        if len(plot_not_in_db) != 0:
            raise IncoherentDatabaseStateError(
                "Tried to insert plot_occurrence records with plot records "
                "that do not exist in database."
            )
        if len(occ_not_in_db) != 0:
            raise IncoherentDatabaseStateError(
                "Tried to insert plot_occurrence records with occurrence "
                "records that dot not exist in database."
            )
        dataframe = dataframe.merge(
            plot_ids,
            left_on='provider_plot_pk',
            right_index=True,
        )
        dataframe = dataframe.merge(
            occ_ids,
            left_on='provider_occurrence_pk',
            right_index=True,
        )
        dataframe.set_index(['plot_id', 'occurrence_id'], inplace=True)
        dataframe['provider_id'] = self.data_provider.db_id
        dataframe['plot_id'] = dataframe.index.get_level_values('plot_id')
        dataframe['occurrence_id'] = dataframe.index.get_level_values(
            'occurrence_id'
        )
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
            df = niamoto_dataframe
            df['plot_id'] = df.index.get_level_values('plot_id')
            df['occurrence_id'] = df.index.get_level_values('occurrence_id')
            return df
        diff = niamoto_dataframe.index.difference(provider_dataframe.index)
        return niamoto_dataframe.loc[diff]
