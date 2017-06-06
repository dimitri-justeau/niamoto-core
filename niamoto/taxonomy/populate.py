# coding: utf-8

import os

import pandas as pd

from niamoto.taxonomy.taxonomy_manager import TaxonomyManager

PATH = os.path.abspath(os.path.dirname(__file__))
NCPIPPN_REF = os.path.join(PATH, 'data', 'ncpippn.json')


def load_ncpippn_taxon_dataframe_from_json(path=NCPIPPN_REF):
    return pd.read_json(path)


def populate_ncpippn_taxon_database(dataframe):
    """
    Populate a Niamoto database with a taxonomic referential.
    :param dataframe: The dataframe containing the taxonomic referential.
    """
    TaxonomyManager.set_taxonomy(dataframe)
