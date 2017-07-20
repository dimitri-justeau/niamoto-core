# coding: utf-8

from datetime import datetime
import time

from sqlalchemy import select, func, bindparam, Index, cast
from sqlalchemy.dialects.postgresql import JSONB
import pandas as pd

from niamoto.db.connector import Connector
from niamoto.db import metadata as meta
from niamoto.exceptions import MalformedDataSourceError, \
    NoRecordFoundError, RecordAlreadyExistsError
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


class TaxonomyManager:
    """
    Class methods for managing the taxonomy.
    """

    IDENTITY_SYNONYM_KEY = 'niamoto'

    @classmethod
    def get_raw_taxon_dataframe(cls):
        """
        :return: A pandas DataFrame containing all the taxon data available
        within the given database.
        """
        with Connector.get_connection() as connection:
            sel = select([meta.taxon])
            return pd.read_sql(
                sel,
                connection,
                index_col=meta.taxon.c.id.name,
            )

    @classmethod
    def delete_all_taxa(cls, bind=None):
        """
        Delete all the taxa stored in the given database.
        :param bind If passed, use an existing engine or connection.
        """
        LOGGER.debug("Deleting all taxa...")
        delete = meta.taxon.delete()
        if bind is not None:
            result = bind.execute(delete)
        else:
            with Connector.get_connection() as connection:
                result = connection.execute(delete)
        LOGGER.debug("{} taxa had been deleted.".format(result.rowcount))

    @classmethod
    def set_taxonomy(cls, taxon_dataframe):
        """
        Set the taxonomy. If a taxonomy already exist, delete it and set the
        new one.
        :param taxon_dataframe: A dataframe containing the taxonomy to set.
        The dataframe must contain at least the following columns:
            taxon_id  -> The id of the taxon (must be the index of the
                         DataFrame).
            parent_id -> The id of the taxon's parent.
            rank      -> The rank of the taxon (must be a value in:
                         'REGNUM', 'PHYLUM', 'CLASSIS', 'ORDO', 'FAMILIA',
                         'GENUS', 'SPECIES', 'INFRASPECIES')
            full_name -> The full name of the taxon.
            rank_name -> The rank name of the taxon.
        The remaining columns will be stored as synonyms, using the column
        name.
        """
        LOGGER.debug("Starting set_taxonomy...")
        required_columns = {'parent_id', 'rank', 'full_name', 'rank_name'}
        cols = set(list(taxon_dataframe.columns))
        inter = cols.intersection(required_columns)
        synonym_cols = cols.difference(required_columns)
        if cls.IDENTITY_SYNONYM_KEY in synonym_cols:
            synonym_cols = synonym_cols.difference({cls.IDENTITY_SYNONYM_KEY})
            m = "The '{}' synonym key is a special key reserved by Niamoto," \
                "this column will be ignored."
            LOGGER.warning(m.format(cls.IDENTITY_SYNONYM_KEY))
        if not inter == required_columns:
            m = "The taxon dataframe does not contains the required " \
                "columns {}, csv has: {}".format(required_columns, cols)
            raise MalformedDataSourceError(m)
        if len(synonym_cols) > 0:
            LOGGER.debug(
                "The following synonym keys had been detected: {}".format(
                    ','.join(synonym_cols)
                )
            )
            synonyms = taxon_dataframe[list(synonym_cols)].apply(
                lambda x: x.to_json(),
                axis=1
            )
        else:
            LOGGER.debug("No synonym keys had been detected.")
            synonyms = '{}'
        taxon_dataframe.drop(synonym_cols, axis=1, inplace=True)
        taxon_dataframe['synonyms'] = synonyms
        mptt = ['mptt_tree_id', 'mptt_depth', 'mptt_left', 'mptt_right']
        for col in mptt:
            taxon_dataframe[col] = 0
        taxon_dataframe = cls.construct_mptt(taxon_dataframe)
        taxon_dataframe['taxon_id'] = taxon_dataframe.index
        taxon_dataframe = taxon_dataframe.astype(object).where(
            pd.notnull(taxon_dataframe), None
        )
        with Connector.get_connection() as connection:
            current_synonym_keys = set(pd.read_sql(select([
                meta.synonym_key_registry.c.name
            ]), connection)['name'])
            to_add = synonym_cols.difference(current_synonym_keys)
            to_delete = current_synonym_keys.difference(
                synonym_cols
            ).difference({cls.IDENTITY_SYNONYM_KEY})
            to_keep = current_synonym_keys.intersection(synonym_cols)
            if len(to_delete) > 0:
                prov = meta.data_provider
                syno = meta.synonym_key_registry
                providers_to_update = pd.read_sql(
                    select([prov.c.name]).select_from(
                        prov.join(syno, syno.c.id == prov.c.synonym_key_id)
                    ).where(syno.c.name.in_(to_delete)),
                    connection
                )
                msg = "The following synonym keys will be deleted: {}. " \
                      "The following data providers where depending on those" \
                      " synonym keys: {} Please consider updating " \
                      "them, or updating the taxonomy."
                LOGGER.warning(msg.format(
                    to_delete,
                    set(providers_to_update['name']))
                )
            with connection.begin():
                connection.execute("SET CONSTRAINTS ALL DEFERRED;")
                # Unregister synonym keys
                cls.unregister_all_synonym_keys(
                    bind=connection,
                    exclude=to_keep,
                )
                # Delete existing taxonomy
                cls.delete_all_taxa(bind=connection)
                # Register synonym cols
                for synonym_key in to_add:
                    cls.register_synonym_key(synonym_key, bind=connection)
                # Insert the data
                LOGGER.debug("Inserting the taxonomy in database...")
                if len(taxon_dataframe) > 0:
                    ins = meta.taxon.insert().values(
                        id=bindparam('taxon_id'),
                        full_name=bindparam('full_name'),
                        rank_name=bindparam('rank_name'),
                        rank=bindparam('rank'),
                        parent_id=bindparam('parent_id'),
                        synonyms=cast(bindparam('synonyms'), JSONB),
                        mptt_left=bindparam('mptt_left'),
                        mptt_right=bindparam('mptt_right'),
                        mptt_tree_id=bindparam('mptt_tree_id'),
                        mptt_depth=bindparam('mptt_depth'),
                    )
                    result = connection.execute(
                        ins,
                        taxon_dataframe.to_dict(orient='records')
                    ).rowcount
                else:
                    result = 0
                m = "The taxonomy had been successfully set ({} taxa " \
                    "inserted)!"
                LOGGER.debug(m.format(result))
        return result, synonym_cols

    def set_synonym_data(self, synonym_key, data):
        pass  # TODO

    @classmethod
    def get_synonym_keys(cls):
        """
        :return: The synonym keys from the database in a pandas DataFrame.
        """
        with Connector.get_connection() as connection:
            sel = select([meta.synonym_key_registry])
            return pd.read_sql(
                sel,
                connection,
                index_col=meta.synonym_key_registry.c.id.name,
            )

    @classmethod
    def get_synonym_key(cls, synonym_key, bind=None):
        sel = select([meta.synonym_key_registry]).where(
            meta.synonym_key_registry.c.name == synonym_key
        )
        if bind is not None:
            result = bind.execute(sel)
        else:
            with Connector.get_connection() as connection:
                result = connection.execute(sel)
        if result.rowcount == 0:
            m = "The synonym key '{}' does not exist in database."
            raise NoRecordFoundError(m.format(synonym_key))
        return result.fetchone()

    @classmethod
    def register_synonym_key(cls, synonym_key, bind=None):
        """
        Register a synonym key in database.
        :param synonym_key: The synonym key to register
        :param bind: If passed, use and existing engine or connection.
        """
        now = datetime.now()
        ins = meta.synonym_key_registry.insert({
            'name': synonym_key,
            'date_create': now,
        })
        if bind is not None:
            cls.assert_synonym_key_does_not_exists(synonym_key, bind=bind)
            bind.execute(ins)
            cls._register_unique_synonym_key_constraint(synonym_key, bind=bind)
            return
        with Connector.get_connection() as connection:
            cls.assert_synonym_key_does_not_exists(
                synonym_key,
                bind=connection
            )
            connection.execute(ins)
            cls._register_unique_synonym_key_constraint(
                synonym_key,
                bind=connection
            )
        LOGGER.debug("synonym_key {} registered.".format(synonym_key))

    @classmethod
    def unregister_synonym_key(cls, synonym_key, bind=None):
        """
        Unregister a synonym key from database.
        :param synonym_key: The synonym key to unregister
        :param bind: If passed, use and existing engine or connection.
        """
        delete_stmt = meta.synonym_key_registry.delete().where(
            meta.synonym_key_registry.c.name == synonym_key
        )
        if bind is not None:
            cls.assert_synonym_key_exists(synonym_key, bind=bind)
            bind.execute(delete_stmt)
            cls._unregister_unique_synonym_key_constraint(
                synonym_key,
                bind=bind
            )
            return
        with Connector.get_connection() as connection:
            cls.assert_synonym_key_exists(synonym_key, bind=connection)
            connection.execute(delete_stmt)
            cls._unregister_unique_synonym_key_constraint(
                synonym_key,
                bind=connection
            )
        LOGGER.debug(
            "synonym_key {} unregistered.".format(synonym_key)
        )

    @classmethod
    def unregister_all_synonym_keys(cls, exclude=[], bind=None):
        """
        Unregister all the synonym keys from database.
        :param exclude: A list of synonym keys to exclude.
        :param bind: If passed, use and existing engine or connection.
        """
        close_after = False
        if bind is None:
            close_after = True
            bind = Connector.get_engine().connect()
        identity = cls.IDENTITY_SYNONYM_KEY
        for synonym_key in cls.get_synonym_keys()['name']:
            if synonym_key not in exclude and synonym_key != identity:
                cls.unregister_synonym_key(synonym_key, bind=bind)
        LOGGER.debug("All synonym_key records unregistered.")
        if close_after:
            bind.close()

    @classmethod
    def _register_unique_synonym_key_constraint(cls, synonym_key, bind=None):
        """
        :param bind: If passed, use an existing connection or engine instead
        of creating a new one.
        """
        synonym_col = meta.taxon.c.synonyms[synonym_key]
        index = Index(
            "{}_unique_synonym_key".format(synonym_key),
            synonym_col,
            unique=True,
            postgresql_where=(
                synonym_col != 'null'
            )
        )
        if bind is None:
            bind = Connector.get_engine()
        index.create(bind)
        meta.taxon.indexes.remove(index)
        LOGGER.debug(
            "{}_unique_synonym_key registered.".format(synonym_key)
        )

    @classmethod
    def _unregister_unique_synonym_key_constraint(cls, synonym_key, bind=None):
        synonym_col = meta.taxon.c.synonyms[synonym_key]
        index = Index(
            "{}_unique_synonym_key".format(synonym_key),
            synonym_col,
            unique=True,
            postgresql_where=(
                synonym_col != 'null'
            )
        )
        meta.taxon.indexes.remove(index)
        if bind is None:
            bind = Connector.get_engine()
        index.drop(bind)
        LOGGER.debug(
            "{}_unique_synonym_key unregistered.".format(synonym_key)
        )

    @classmethod
    def add_synonym_for_single_taxon(cls, taxon_id, synonym_key,
                                     provider_taxon_id):
        """
        For a single taxon, add the synonym corresponding to a synonym key.
        :param taxon_id: The id of the taxon (in Niamoto's referential).
        :param synonym_key: The synonym_key.
        :param provider_taxon_id: The id of the taxon in the provider's
        referential, i.e. the synonym.
        """
        upd = meta.taxon.update().where(
            meta.taxon.c.id == taxon_id
        ).values(
            {
                'synonyms': meta.taxon.c.synonyms.concat(
                    func.jsonb_build_object(
                        synonym_key,
                        provider_taxon_id
                    )
                )
            }
        )
        with Connector.get_connection() as connection:
            cls.assert_synonym_key_exists(synonym_key, bind=connection)
            connection.execute(upd)

    @classmethod
    def get_synonyms_for_key(cls, synonym_key):
        """
        :param synonym_key: The synonym key to consider. If synonym key is
        'niamoto', return the niamoto id's (identity synonym).
        :return: A Series with index corresponding to the data provider's
        taxa ids, and values corresponding to their synonym in Niamoto's
        referential.
        """
        with Connector.get_connection() as connection:
            niamoto_id_col = meta.taxon.c.id
            synonym_col = meta.taxon.c.synonyms
            if synonym_key == cls.IDENTITY_SYNONYM_KEY:
                sel = select([
                    niamoto_id_col.label("niamoto_taxon_id"),
                    niamoto_id_col.label("provider_taxon_id"),
                ])
            else:
                sel = select([
                    niamoto_id_col.label("niamoto_taxon_id"),
                    synonym_col[synonym_key].label("provider_taxon_id"),
                ]).where(synonym_col[synonym_key].isnot(None))
            synonyms = pd.read_sql(
                sel,
                connection,
                index_col="provider_taxon_id"
            )["niamoto_taxon_id"]
            return synonyms[synonyms.index.notnull()]

    @staticmethod
    def assert_synonym_key_exists(synonym_key, bind=None):
        """
        Assert the existence of a synonym key in synonym key registry.
        :param synonym_key: The synonym key to check.
        :param bind: If passed, use an existing engine or connection.
        """
        sel = select([meta.synonym_key_registry]).where(
            meta.synonym_key_registry.c.name == synonym_key
        )
        if bind is not None:
            result = bind.execute(sel).rowcount
        else:
            with Connector.get_connection() as connection:
                result = connection.execute(sel).rowcount
        if result == 0:
            m = "The synonym key '{}' does not exist in database."
            raise NoRecordFoundError(m.format(synonym_key))

    @staticmethod
    def assert_synonym_key_does_not_exists(synonym_key, bind=None):
        """
        Assert the non-existence of a synonym key in synonym key registry.
        :param synonym_key: The synonym key to check.
        :param bind: If passed, use an existing engine or connection.
        """
        sel = select([meta.synonym_key_registry]).where(
            meta.synonym_key_registry.c.name == synonym_key
        )
        if bind is not None:
            result = bind.execute(sel).rowcount
        else:
            with Connector.get_connection() as connection:
                result = connection.execute(sel).rowcount
        if result > 0:
            m = "The synonym key '{}' already exists in database."
            raise RecordAlreadyExistsError(m.format(synonym_key))

    @classmethod
    def make_mptt(cls):
        """
        Build the mptt in database.
        """
        df = cls.get_raw_taxon_dataframe()
        mptt = cls.construct_mptt(df)
        mptt['taxon_id'] = mptt.index
        upd = meta.taxon.update().where(
            meta.taxon.c.id == bindparam('taxon_id')
        ).values({
            'mptt_tree_id': bindparam('mptt_tree_id'),
            'mptt_depth': bindparam('mptt_depth'),
            'mptt_left': bindparam('mptt_left'),
            'mptt_right': bindparam('mptt_right'),
        })
        with Connector.get_connection() as connection:
            connection.execute(
                upd,
                mptt[[
                    'taxon_id',
                    'mptt_tree_id',
                    'mptt_depth',
                    'mptt_left',
                    'mptt_right',
                ]].to_dict(orient='records')
            )

    @staticmethod
    def construct_mptt(dataframe):
        """
        Given a taxa DataFrame, Construct the mptt (modified pre order tree
        traversal) and return it as a DataFrame.
        :param dataframe: A pandas DataFrame of taxa. The 'parent_id' must be
        filled since the method will rely on it to build the mptt.
        :return: The built mptt.
        """
        LOGGER.debug("Constructing the MPTT tree...")
        t = time.time()
        df = dataframe.copy()
        # Find roots
        roots = dataframe[pd.isnull(dataframe['parent_id'])]
        for i, root in roots.iterrows():
            df.loc[i, 'mptt_tree_id'] = i
            df.loc[i, 'mptt_depth'] = 0
            df.loc[i, 'mptt_left'] = 1
            right = TaxonomyManager._construct_tree(df, i, 1, 1)
            df.loc[i, 'mptt_right'] = right
        m = "The MPTT tree had been successfully constructed ({:.2f} s)!"
        LOGGER.debug(m.format(time.time() - t))
        return df

    @staticmethod
    def _construct_tree(df, parent_id, depth, left):
        """
        Recursive method that builds the subtree under a given taxon.
        Writes the attributes in the DataFrame given as parameter.
        :param df: The taxon DataFrame.
        :param parent_id: The id of the taxon to build the subtree for.
        :param depth: The depth for the first level of the subtree.
        :param left: The starting left attribute.
        :return: The final right attribute.
        """
        children = df[df['parent_id'] == parent_id]
        right = left + 1
        if len(children) == 0:
            return right
        for i, child in children.iterrows():
            df.loc[i, 'mptt_tree_id'] = df.loc[parent_id]['mptt_tree_id']
            df.loc[i, 'mptt_depth'] = depth
            df.loc[i, 'mptt_left'] = right
            right = TaxonomyManager._construct_tree(df, i, depth + 1, right)
            df.loc[i, 'mptt_right'] = right
            right += 1
        return right
