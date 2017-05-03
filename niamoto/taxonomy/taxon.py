# coding: utf-8

from sqlalchemy import select, func
import pandas as pd

from niamoto import settings
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
    def get_raw_taxon_dataframe(cls, database=settings.DEFAULT_DATABASE):
        with Connector.get_connection(database=database) as connection:
            sel = select([niamoto_db_meta.taxon])
            return pd.read_sql(
                sel,
                connection,
                index_col=niamoto_db_meta.taxon.c.id.name,
            )

    @classmethod
    def delete_all_taxa(cls, database=settings.DEFAULT_DATABASE):
        with Connector.get_connection(database=database) as connection:
            delete = niamoto_db_meta.taxon.delete()
            connection.execute(delete)

    @classmethod
    def add_synonym_for_single_taxon(cls, taxon_id, data_provider,
                                     provider_taxon_id,
                                     database=settings.DEFAULT_DATABASE):
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
        with Connector.get_connection(database=database) as connection:
            connection.execute(upd)

    @classmethod
    def make_mptt(cls):
        pass

    @staticmethod
    def construct_mptt(dataframe):
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
