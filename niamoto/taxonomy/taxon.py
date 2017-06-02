# coding: utf-8

from sqlalchemy import select, func, bindparam
import pandas as pd

from niamoto.db.connector import Connector
from niamoto.db import metadata as niamoto_db_meta


class Taxon:
    """
    Class representing a taxon and implementing class methods to retrieve
    and manipulate the taxonomic tree.
    """

    def __init__(self):
        pass

    @classmethod
    def get_raw_taxon_dataframe(cls):
        """
        :return: A pandas DataFrame containing all the taxon data available
        within the given database.
        """
        with Connector.get_connection() as connection:
            sel = select([niamoto_db_meta.taxon])
            return pd.read_sql(
                sel,
                connection,
                index_col=niamoto_db_meta.taxon.c.id.name,
            )

    @classmethod
    def delete_all_taxa(cls):
        """
        Delete all the taxa stored in the given database.
        """
        with Connector.get_connection() as connection:
            delete = niamoto_db_meta.taxon.delete()
            connection.execute(delete)

    @classmethod
    def add_synonym_for_single_taxon(cls, taxon_id, data_provider,
                                     provider_taxon_id):
        """
        For a single taxon, add the synonym corresponding to a data provider.
        :param taxon_id: The id of the taxon (in Niamoto's referential).
        :param data_provider: The data provider corresponding to the synonym to
        add. Since the synonym is tagged with the provider's type, which is a
        class attribute, this parameter can either be an instance or a class.
        :param provider_taxon_id: The id of the taxon in the provider's
        referential, i.e. the synonym.
        """
        upd = niamoto_db_meta.taxon.update().where(
            niamoto_db_meta.taxon.c.id == taxon_id
        ).values(
            {
                'synonyms': niamoto_db_meta.taxon.c.synonyms.concat(
                    func.jsonb_build_object(
                        data_provider.get_type_name(),
                        provider_taxon_id
                    )
                )
            }
        )
        with Connector.get_connection() as connection:
            connection.execute(upd)

    @classmethod
    def get_synonyms_for_provider(cls, data_provider):
        """
        :param data_provider: The data provider corresponding to the synonym to
        get. Since the synonym is tagged with the provider's type, which is a
        class attribute, this parameter can either be an instance or a class.
        :return: A Series with index corresponding to the data provider's
        taxa ids, and values corresponding to their synonym in Niamoto's
        referential.
        """
        with Connector.get_connection() as connection:
            provider_type = data_provider.get_type_name()
            niamoto_id_col = niamoto_db_meta.taxon.c.id
            synonym_col = niamoto_db_meta.taxon.c.synonyms
            sel = select([
                niamoto_id_col.label("niamoto_taxon_id"),
                synonym_col[provider_type].label("provider_taxon_id"),
            ]).where(synonym_col[provider_type].isnot(None))
            synonyms = pd.read_sql(
                sel,
                connection,
                index_col="provider_taxon_id"
            )["niamoto_taxon_id"]
            return synonyms

    @classmethod
    def make_mptt(cls):
        """
        Build the mptt in database.
        """
        df = cls.get_raw_taxon_dataframe()
        mptt = cls.construct_mptt(df)
        mptt['taxon_id'] = mptt.index
        upd = niamoto_db_meta.taxon.update().where(
            niamoto_db_meta.taxon.c.id == bindparam('taxon_id')
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
        df = dataframe.copy()
        # Find roots
        roots = dataframe[pd.isnull(dataframe['parent_id'])]
        for i, root in roots.iterrows():
            df.loc[i, 'mptt_tree_id'] = i
            df.loc[i, 'mptt_depth'] = 0
            df.loc[i, 'mptt_left'] = 1
            right = Taxon._construct_tree(df, i, 1, 1)
            df.loc[i, 'mptt_right'] = right
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
            right = Taxon._construct_tree(df, i, depth + 1, right)
            df.loc[i, 'mptt_right'] = right
            right += 1
        return right
