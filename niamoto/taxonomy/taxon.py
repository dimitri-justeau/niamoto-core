# coding: utf-8

from sqlalchemy import select
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
    def get_raw_taxon_dataset(cls, database=settings.DEFAULT_DATABASE):
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
