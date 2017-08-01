# coding: utf-8

import os

import pandas as pd
from sqlalchemy import select

from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.api.data_provider_api import get_data_provider_list
from niamoto.data_providers.base_data_provider import PROVIDER_REGISTRY
from niamoto.exceptions import DataSourceNotFoundError
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def set_taxonomy(csv_file_path):
    """
    Set the niamoto taxonomy from a csv file.
    The csv must have a header and it must contains at least the following
    columns:
    - id: The unique identifier of the taxon, in the provider’s referential.
    - parent_id: The parent’s id of the taxon. If the taxon is a root, let the
                 value blank.
    - rank: The rank of the taxon, can be a value among: ‘REGNUM’, ‘PHYLUM’,
            ‘CLASSIS’, ‘ORDO’, ‘FAMILIA’, ‘GENUS’, ‘SPECIES’, ‘INFRASPECIES’.
    - full_name: The full name of the taxon.
    - rank_name: The rank name of the taxon.
    All the additional columns will be considered as synonyms, their values
    must therefore be integers corresponding to the corresponding value in the
    referential pointed by the synonym key.
    :param csv_file_path: The csv file path.
    :return: (number_of_taxon_inserted, synonyms_registered)
    """
    if not os.path.exists(csv_file_path) or os.path.isdir(csv_file_path):
        raise DataSourceNotFoundError(
            "The csv file '{}' had not been found.".format(csv_file_path)
        )
    dataframe = pd.DataFrame.from_csv(csv_file_path, index_col='id')
    return TaxonomyManager.set_taxonomy(dataframe)


def map_all_synonyms():
    """
    Update the synonym mapping for every data provider registered in the
    database.
    """
    data_providers = get_data_provider_list()
    for i, record in data_providers.iterrows():
        name = record['name']
        provider_type = record['provider_type']
        synonym_key = record['synonym_key']
        data_provider = PROVIDER_REGISTRY[provider_type]['class'](name)
        m, s = data_provider.occurrence_provider.update_synonym_mapping()
        msg = "DataProvider(provider_type='{}', name='{}', synonym_key='{}'" \
              "): {} taxa had been mapped, over {} occurrences."
        LOGGER.info(msg.format(
            provider_type,
            name,
            synonym_key,
            len(m[m.notnull()]),
            len(m),
        ))
    return data_providers


def get_synonym_keys():
    """
    :return: A Dataframe containing all the registered synonym keys.
    """
    sel = select([niamoto_db_meta.synonym_key_registry])
    with Connector.get_connection() as connection:
        df = pd.read_sql(sel, connection, index_col='id')
    return df
