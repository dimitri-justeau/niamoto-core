# coding: utf-8

from sqlalchemy.sql import select
import pandas as pd

from niamoto.db.metadata import occurrence
from niamoto.db.connector import Connector
from niamoto.settings import DEFAULT_DATABASE, NIAMOTO_SCHEMA


class BaseOccurrenceProvider:
    """
    Abstract base class for occurrence provider.
    """

    def __init__(self, db_id):
        """
        :param db_id: The database id of the corresponding data
        provider record.
        """
        self.db_id = db_id

    def get_current_occurrence_data(self, database=DEFAULT_DATABASE,
                                    schema=NIAMOTO_SCHEMA):
        """
        :return: A DataFrame containing the current database occurrence data
        for this provider.
        """
        with Connector.get_connection(
                database=database,
                schema=schema
        ) as connection:
            sel = select([occurrence]).where(
                occurrence.c.provider_id == self.db_id
            )
            return pd.read_sql(sel, connection)
