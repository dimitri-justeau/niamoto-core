# coding: utf-8

from sqlalchemy import select, cast, String
import pandas as pd

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector


class TaxonDataPublisher(BaseDataPublisher):
    """
    Publish plot dataframe.
    """

    @classmethod
    def get_key(cls):
        return 'taxa'

    @classmethod
    def get_description(cls):
        return "Publish the taxa dataframe."

    def _process(self, *args, include_mptt=False, include_synonyms=False,
                 flatten=False, **kwargs):
        """
        Return the taxon dataframe.
        :param include_mptt: If True, include the mptt columns.
        :param include_synonyms: If True, include the stored synonyms for each
            taxon.
        :param flatten: If True, flattens the taxonomy hierarchy and include
            it in the resulting dataframe.
        """
        with Connector.get_connection() as connection:
            keys = TaxonomyManager.get_synonym_keys()['name']
            synonyms = []
            if include_synonyms:
                synonyms = [meta.taxon.c.synonyms[k].label(k) for k in keys
                            if k != 'niamoto']
            mptt = []
            if include_mptt:
                mptt = [
                    meta.taxon.c.mptt_left.label('mptt_left'),
                    meta.taxon.c.mptt_right.label('mptt_right'),
                    meta.taxon.c.mptt_tree_id.label('mptt_tree_id'),
                    meta.taxon.c.mptt_depth.label('mptt_depth'),
                ]
            sel = select([
                meta.taxon.c.id.label('id'),
                meta.taxon.c.full_name.label('full_name'),
                meta.taxon.c.rank_name.label('rank_name'),
                cast(meta.taxon.c.rank, String).label('rank'),
                meta.taxon.c.parent_id.label('parent_id'),
            ] + synonyms + mptt)
            df = pd.read_sql(sel, connection, index_col='id')
            #  Replace None values with nan
            df.fillna(value=pd.np.NAN, inplace=True)
            if flatten:
                df = _flatten(df)
            return df, [], {'index_label': 'id'}

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.SQL]


def _flatten(df):
    ranks = [opt.value.lower() for opt in meta.TaxonRankEnum]
    for r in ranks:
        df[r] = None

    def _flatten_row(row):
        row[row['rank'].lower()] = row['full_name']
        parent_id = row['parent_id']
        while pd.notnull(parent_id):
            parent = df.loc[parent_id]
            row[parent['rank'].lower()] = parent['full_name']
            parent_id = parent['parent_id']
        return row

    return df.apply(_flatten_row, axis=1)
