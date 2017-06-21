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

    def _process(self, *args, include_mptt=False, **kwargs):
        """
        :param include_mptt: If True, include the mptt columns.
        """
        with Connector.get_connection() as connection:
            keys = TaxonomyManager.get_synonym_keys()['name']
            synonyms = [meta.taxon.c.synonyms[k].label(k) for k in keys
                        if k != 'niamoto']
            mptt = []
            if include_mptt:
                mptt = [
                    meta.taxon.c.mptt_left,
                    meta.taxon.c.mptt_right,
                    meta.taxon.c.mptt_tree_id,
                    meta.taxon.c.mptt_depth
                ]
            sel = select([
                meta.taxon.c.id.label('id'),
                meta.taxon.c.full_name.label('full_name'),
                meta.taxon.c.rank_name.label('rank_name'),
                cast(meta.taxon.c.rank, String).label('rank'),
                meta.taxon.c.parent_id.label('parent_id'),
                meta.taxon.c.rank_name.label('rank_name'),
            ] + synonyms + mptt)
            df = pd.read_sql(sel, connection, index_col='id')
            #  Replace None values with nan
            df.fillna(value=pd.np.NAN, inplace=True)
            return df, [], {'index_label': 'id'}

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.STREAM]

