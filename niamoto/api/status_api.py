# coding: utf-8

from sqlalchemy import select, func
import pandas as pd

from niamoto.db.connector import Connector
from niamoto.db import metadata as meta


def get_general_status():
    """
    :return: A dict containing general information on the current Niamoto
    database state:
        - nb_occurrences
        - nb_plots
        - nb_plots_occurrences
        - nb_data_providers
        - nb_taxa
        - nb_synonym_keys
        - nb_rasters
    """
    with Connector.get_connection() as connection:
        sel = select([
            select([
                func.count(meta.occurrence.c.id)
            ]).label('nb_occurrences'),
            select([
                func.count(meta.plot.c.id)
            ]).label('nb_plots'),
            select([
                func.count(func.distinct(
                    meta.plot_occurrence.c.plot_id,
                    meta.plot_occurrence.c.occurrence_id)
                )
            ]).label('nb_plots_occurrences'),
            select([
                func.count(meta.data_provider.c.id)
            ]).label('nb_data_providers'),
            select([
                func.count(meta.taxon.c.id)
            ]).label('nb_taxa'),
            select([
                func.count(meta.synonym_key_registry.c.id)
            ]).label(
                'nb_synonym_keys'
            ),
            select([
                func.count(meta.raster_registry.c.name)
            ]).label('nb_rasters'),
            select([
                func.count(meta.vector_registry.c.name)
            ]).label('nb_vectors'),
        ])
        df = pd.read_sql(sel, connection)
    return {
        'nb_occurrences': df.iloc[0]['nb_occurrences'],
        'nb_plots': df.iloc[0]['nb_plots'],
        'nb_plots_occurrences': df.iloc[0]['nb_plots_occurrences'],
        'nb_data_providers': df.iloc[0]['nb_data_providers'],
        'nb_taxa': df.iloc[0]['nb_taxa'],
        'nb_synonym_keys': df.iloc[0]['nb_synonym_keys'],
        'nb_rasters': df.iloc[0]['nb_rasters'],
        'nb_vectors': df.iloc[0]['nb_vectors'],
    }
