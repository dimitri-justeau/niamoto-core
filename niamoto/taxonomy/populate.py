# coding: utf-8

import os

from sqlalchemy import bindparam, func
import pandas as pd

from niamoto.taxonomy.taxon import Taxon
from niamoto.db.connector import Connector
from niamoto.db import metadata as niamoto_db_meta
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider


PATH = os.path.abspath(os.path.dirname(__file__))
NCPIPPN_REF = os.path.join(PATH, 'data', 'ncpippn.json')


def load_ncpippn_taxon_dataframe_from_json(path=NCPIPPN_REF):
    return pd.read_json(path)


def populate_ncpippn_taxon_database(dataframe):
    """
    Populate a Niamoto database with a taxonomic referential.
    :param dataframe: The dataframe containing the taxonomic referential.
    """
    dataframe['tax_id'] = dataframe.index
    dataframe = dataframe.astype(object).where(pd.notnull(dataframe), None)
    ins = niamoto_db_meta.taxon.insert().values(
        id=bindparam('tax_id'),
        full_name=bindparam('full_name'),
        rank_name=bindparam('rank_name'),
        rank=bindparam('rank'),
        parent_id=bindparam('parent'),
        synonyms=func.jsonb_build_object(
            'taxref', bindparam('id_taxref'),
            PlantnoteDataProvider.get_type_name(), bindparam('tax_id'),
        ),
        mptt_left=0,
        mptt_right=0,
        mptt_tree_id=0,
        mptt_depth=0,
    )
    levels = [
        niamoto_db_meta.TaxonRankEnum.REGNUM.value,
        niamoto_db_meta.TaxonRankEnum.PHYLUM.value,
        niamoto_db_meta.TaxonRankEnum.CLASSIS.value,
        niamoto_db_meta.TaxonRankEnum.ORDO.value,
        niamoto_db_meta.TaxonRankEnum.FAMILIA.value,
        niamoto_db_meta.TaxonRankEnum.GENUS.value,
        niamoto_db_meta.TaxonRankEnum.SPECIES.value,
        niamoto_db_meta.TaxonRankEnum.INFRASPECIES.value,
    ]
    for l in levels:
        with Connector.get_connection() as connection:
            df = dataframe[dataframe['rank'] == l]
            if len(df) > 0:
                connection.execute(ins, df.to_dict(orient='records'))
    Taxon.make_mptt()
