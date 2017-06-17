# coding: utf-8

import time
import json
import io

from sqlalchemy.sql import select, bindparam, and_, cast, func
from sqlalchemy.dialects.postgresql import JSONB
import pandas as pd

from niamoto.conf import settings
from niamoto.db.connector import Connector
from niamoto.db.metadata import occurrence
from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


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
        LOGGER.debug("Getting Niamoto occurrence dataframe...")
        sel = select([
            occurrence.c.id,
            occurrence.c.provider_id,
            occurrence.c.provider_pk,
            func.st_asewkt(occurrence.c.location).label('location'),
            occurrence.c.taxon_id,
            occurrence.c.provider_taxon_id,
            occurrence.c.properties,
        ]).where(
            occurrence.c.provider_id == self.data_provider.db_id
        )
        return pd.read_sql(
            sel,
            connection,
            index_col=occurrence.c.id.name,
        )

    def update_synonym_mapping(self, connection=None):
        """
        Update the synonym mapping of an already stored dataframe.
        To be called when a synonym had been defined or modified, but not
        the occurrences.
        :param connection: If passed, use an existing connection.
        """
        # Log start
        m = "(provider_id='{}', synonym_key='{}'): Updating synonym " \
            "mapping..."
        LOGGER.debug(m.format(
            self.data_provider.db_id,
            self.data_provider.synonym_key)
        )
        close_after = False
        if connection is None:
            connection = Connector.get_engine().connect()
            close_after = True
        # Start
        df = self.get_niamoto_occurrence_dataframe(connection)
        if close_after:
            connection.close()
        synonyms = TaxonomyManager.get_synonyms_for_key(
            self.data_provider.synonym_key
        )
        mapping = df["provider_taxon_id"].map(synonyms)
        if len(df) > 0:
            df["taxon_id"] = mapping
            df = df[['provider_id', 'provider_pk', 'taxon_id']]
            s = io.StringIO()
            df.where((pd.notnull(df)), None).rename(columns={
                'provider_id': 'prov_id',
                'provider_pk': 'prov_pk',
            }).to_csv(s, columns=['taxon_id', 'prov_id', 'prov_pk'])
            s.seek(0)
            sql_create_temp = \
                """
                DROP TABLE IF EXISTS {tmp};
                CREATE TABLE {tmp} (
                    id float,
                    taxon_id float,
                    prov_id float,
                    prov_pk float
                );
                """.format(**{
                    'tmp': 'tmp_niamoto'
                })
            sql_copy_from = \
                """
                COPY {tmp} FROM STDIN CSV HEADER DELIMITER ',';
                """.format(**{
                    'tmp': 'tmp_niamoto'
                })
            sql_update = \
                """
                UPDATE {occurrence_table}
                SET taxon_id = {tmp}.taxon_id::integer
                FROM {tmp}
                WHERE {occurrence_table}.provider_id = {tmp}.prov_id::integer
                    AND {occurrence_table}.provider_pk = {tmp}.prov_pk::integer;
                DROP TABLE {tmp};
                """.format(**{
                    'tmp': 'tmp_niamoto',
                    'occurrence_table': '{}.{}'.format(
                        settings.NIAMOTO_SCHEMA, occurrence.name
                    )
                })
            raw_connection = Connector.get_engine().raw_connection()
            cur = raw_connection.cursor()
            cur.execute(sql_create_temp)
            cur.copy_expert(sql_copy_from, s)
            cur.execute(
                sql_update
            )
            cur.close()
            raw_connection.commit()
            raw_connection.close()
        # Log end
        m = "(provider_id='{}', synonym_key='{}'): {} synonym mapping had " \
            "been updated."
        LOGGER.debug(m.format(
            self.data_provider.db_id,
            self.data_provider.synonym_key,
            len(synonyms)
        ))
        return mapping, synonyms

    def map_provider_taxon_ids(self, dataframe):
        """
        Map provider's taxon ids with Niamoto taxon ids when importing data.
        :param dataframe: The dataframe where the mapping has to be done.
        ids. The index must correspond to the provider's pk. The dataframe
        corresponds to the provider's dataframe.
        :return: A series with the same index, the niamoto corresponding
        taxon id as values.
        """
        m = "(provider_id='{}', synonym_key='{}'): " \
            "Mapping provider's taxon ids..."
        LOGGER.debug(m.format(
            self.data_provider.db_id,
            self.data_provider.synonym_key)
        )
        synonyms = TaxonomyManager.get_synonyms_for_key(
            self.data_provider.synonym_key
        )
        dataframe["provider_taxon_id"] = dataframe["taxon_id"]
        dataframe["taxon_id"] = dataframe["taxon_id"].map(synonyms)
        m = "(provider_id='{}', synonym_key='{}'): {} taxon ids had " \
            "been mapped."
        LOGGER.debug(m.format(
            self.data_provider.db_id,
            self.data_provider.synonym_key,
            len(synonyms)
        ))

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
                LOGGER.debug("Inserting new occurrence records...")
                ins_stmt = occurrence.insert().values(
                    provider_id=bindparam('provider_id'),
                    provider_pk=bindparam('provider_pk'),
                    location=bindparam('location'),
                    taxon_id=bindparam('taxon_id'),
                    provider_taxon_id=bindparam('provider_taxon_id'),
                    properties=cast(bindparam('properties'), JSONB),
                )
                ins_data = insert_df.to_dict(orient='records')
                connection.execute(
                    ins_stmt,
                    ins_data
                )
            if len(update_df) > 0:
                LOGGER.debug("Updating existing occurrence records...")
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
                upd_data = update_df.rename(columns={
                    'provider_id': 'prov_id',
                    'provider_pk': 'prov_pk',
                }).to_dict(orient='records')
                connection.execute(
                    upd_stmt,
                    upd_data
                )
            if len(delete_df) > 0:
                LOGGER.debug("Deleting expired occurrence records...")
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
        t = time.time()
        LOGGER.info("** Occurrence sync starting ('{}' - {})...".format(
            self.data_provider.name, self.data_provider.get_type_name()
        ))
        LOGGER.debug("Getting provider's occurrence dataframe...")
        dataframe = self.get_provider_occurrence_dataframe()
        self.map_provider_taxon_ids(dataframe)
        sync_result = self._sync(
            dataframe,
            connection,
            insert=insert,
            update=update,
            delete=delete,
        )
        LOGGER.info("** Occurrence sync with '{}' done ({:.2f} s)!".format(
            self.data_provider.name, time.time() - t
        ))
        return sync_result

    def get_insert_dataframe(self, niamoto_dataframe, provider_dataframe):
        """
        :param niamoto_dataframe: Occurrence DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Occurrence DataFrame from provider.
        :return: The data that is to be inserted to sync Niamoto with the
        provider (i.e. data which is in the provider, but not in Niamoto).
        """
        LOGGER.debug("Resolving occurrence insert dataframe...")
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
        LOGGER.debug("Resolving occurrence update dataframe...")
        niamoto_idx = pd.Index(niamoto_dataframe['provider_pk'])
        inter = provider_dataframe.index.intersection(niamoto_idx)
        if len(inter) > 0:
            prov_df = provider_dataframe.loc[inter]
            niamoto_df = niamoto_dataframe.set_index('provider_pk').loc[inter]
            # Fill na with negative, comparable value
            niamoto_df.fillna(value=-9999, inplace=True)
            prov_df.fillna(value=-9999, inplace=True)
            compared_cols = [
                'taxon_id', 'location', 'properties', 'provider_taxon_id'
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
        :param niamoto_dataframe: Occurrence DataFrame from Niamoto database
        (corresponding to this provider).
        :param provider_dataframe: Occurrence DataFrame from provider.
        :return: The data that is to be deleted to sync Niamoto with the
        provider (i.e. data which is in Niamoto, but not in the provider).
        """
        LOGGER.debug("Resolving occurrence delete dataframe...")
        niamoto_idx = pd.Index(niamoto_dataframe['provider_pk'])
        diff = niamoto_idx.difference(provider_dataframe.index)
        idx = niamoto_dataframe.reset_index().set_index(
            'provider_pk',
        ).loc[diff]['id']
        return niamoto_dataframe.loc[pd.Index(idx)]
