# coding: utf-8

import os
import pandas as pd

from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.api.data_provider_api import get_data_provider_list, \
    PROVIDER_TYPES
from niamoto.exceptions import DataSourceNotFoundError
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def set_taxonomy(csv_file_path):
    """
    Set the niamoto taxonomy from a csv file.
    The csv must have a header and it must contains at least the following
    columns:
    - id: The unique identifier of the taxon, in the provider’s referential.
    - parent_id: The parent’s id of the taxon. If the taxon is a root, let the value blank.
    - rank: The rank of the taxon, can be a value among: ‘REGNUM’, ‘PHYLUM’, ‘CLASSIS’, ‘ORDO’, ‘FAMILIA’, ‘GENUS’, ‘SPECIES’, ‘INFRASPECIES’.
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
        data_provider = PROVIDER_TYPES[provider_type](name)
        m, s = data_provider.occurrence_provider.update_synonym_mapping()
        msg = "{}('{}'): {} taxa had been mapped, over {} occurrences."
        LOGGER.info(msg.format(
            provider_type,
            name,
            len(m[m.notnull()]),
            len(m),
        ))
    return data_providers
