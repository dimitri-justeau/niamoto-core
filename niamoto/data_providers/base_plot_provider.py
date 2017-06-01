# coding: utf-8

import json

from sqlalchemy.sql import select, bindparam, and_, cast, func
from sqlalchemy.dialects.postgresql import JSONB
import pandas as pd

from niamoto.db.metadata import plot
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


class BasePlotProvider:
    """
    Abstract base class for plot provider.
    """

    def __init__(self, data_provider):
        """
        :param data_provider: The parent data provider.
        """
        self.data_provider = data_provider

    def get_niamoto_plot_dataframe(self, connection):
        """
        :param connection: A connection to the database to work with.
        :return: A DataFrame containing the plot data for this
        provider that is currently stored in the Niamoto database.
        """
        LOGGER.debug("Getting Niamoto plot dataframe...")
        sel = select([
            plot.c.id,
            plot.c.provider_id,
            plot.c.provider_pk,
            plot.c.name,
            func.st_asewkt(plot.c.location).label('location'),
            plot.c.properties,
        ]).where(
            plot.c.provider_id == self.data_provider.db_id
        )
        return pd.read_sql(
            sel,
            connection,
            index_col=plot.c.id.name,
        )

    def get_provider_plot_dataframe(self):
        """
        :return: A DataFrame containing the plot data currently
        available from the provider. The index of the DataFrame corresponds
        to the provider's pk.
        The dataframe must be structure with the following columns
            id -> The provider's pk (index).
            name -> The name of the plot.
            location -> Location of the plot.
            properties -> The properties of the plot, JSON format.
        """
        raise NotImplementedError()

    def _sync(self, df, connection, insert=True, update=True, delete=True):
        niamoto_df = self.get_niamoto_plot_dataframe(connection)
        provider_df = df
        insert_df = self.get_insert_dataframe(niamoto_df, provider_df) \
            if insert else pd.DataFrame()
        update_df = self.get_update_dataframe(niamoto_df, provider_df) \
            if update else pd.DataFrame()
        delete_df = self.get_delete_dataframe(niamoto_df, provider_df) \
            if delete else pd.DataFrame()
        with connection.begin():
            if len(insert_df) > 0:
                LOGGER.debug("Inserting new plot records...")
                ins_stmt = plot.insert().values(
                    provider_id=bindparam('provider_id'),
                    provider_pk=bindparam('provider_pk'),
                    name=bindparam('name'),
                    location=bindparam('location'),
                    properties=cast(bindparam('properties'), JSONB),
                )
                connection.execute(
                    ins_stmt,
                    insert_df.to_dict(orient='records')
                )
            if len(update_df) > 0:
                LOGGER.debug("Updating existing plot records...")
                upd_stmt = plot.update().where(
                    and_(
                        plot.c.provider_id == bindparam('prov_id'),
                        plot.c.provider_pk == bindparam('prov_pk')
                    )
                ).values({
                    'location': bindparam('location'),
                    'name': bindparam('name'),
                    'properties': cast(bindparam('properties'), JSONB),
                })
                connection.execute(
                    upd_stmt,
                    update_df.rename(columns={
                        'provider_id': 'prov_id',
                        'provider_pk': 'prov_pk',
                    }).to_dict(orient='records')
                )
            if len(delete_df) > 0:
                LOGGER.debug("Deleting expired plot records...")
                del_stmt = plot.delete().where(
                    plot.c.id.in_(delete_df.index)
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
        LOGGER.debug(">>> Plot sync starting...")
        LOGGER.debug("Getting provider's plot dataframe...")
        df = self.get_provider_plot_dataframe()
        return self._sync(
            df,
            connection,
            insert=insert,
            update=update,
            delete=delete,
        )

    def get_insert_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Plot DataFrame from provider.
        :return: The data that is to be inserted to sync Niamoto with the
        provider (i.e. data which is in the provider, but not in Niamoto).
        """
        LOGGER.debug("Resolving plot insert dataframe...")
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
        LOGGER.debug("Resolving plot update dataframe...")
        niamoto_idx = pd.Index(niamoto_dataframe['provider_pk'])
        inter = provider_dataframe.index.intersection(niamoto_idx)
        if len(inter) > 0:
            prov_df = provider_dataframe.loc[inter]
            niamoto_df = niamoto_dataframe.set_index('provider_pk').loc[inter]
            # Fill na with negative, comparable value
            niamoto_df.fillna(value=-9999, inplace=True)
            prov_df.fillna(value=-9999, inplace=True)
            compared_cols = [
                'name', 'location', 'properties'
            ]
            niamoto_df['properties'] = niamoto_df['properties'].apply(
                lambda x: sorted(json.loads(json.dumps(x)).items())
            )
            prov_df['properties'] = prov_df['properties'].apply(
                lambda x: sorted(json.loads(x).items())
            )
            changed = (
                prov_df[compared_cols] != niamoto_df[compared_cols]
            ).any(1)
            changed_idx = changed[changed].index
            provider_dataframe = provider_dataframe.loc[changed_idx]
        else:
            provider_dataframe = provider_dataframe.loc[inter]
        provider_dataframe['provider_pk'] = provider_dataframe.index
        provider_dataframe['provider_id'] = self.data_provider.db_id
        return provider_dataframe

    def get_delete_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Plot DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Plot DataFrame from provider.
        :return: The data that is to be deleted to sync Niamoto with the
        provider (i.e. data which is in Niamoto, but not in the provider).
        """
        LOGGER.debug("Resolving plot delete dataframe...")
        niamoto_idx = pd.Index(niamoto_dataframe['provider_pk'])
        diff = niamoto_idx.difference(provider_dataframe.index)
        idx = niamoto_dataframe.reset_index().set_index(
            'provider_pk',
        ).loc[diff]['id']
        return niamoto_dataframe.loc[pd.Index(idx)]
