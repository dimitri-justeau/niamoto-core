# coding: utf-8

from sqlalchemy.sql import select
import pandas as pd

from niamoto.db.metadata import plot
from niamoto.db.connector import Connector
from niamoto.settings import DEFAULT_DATABASE, NIAMOTO_SCHEMA


class BasePlotProvider:
    """
    Abstract base class for plot provider.
    """

    def __init__(self, db_id):
        """
        :param db_id: The database id of the corresponding data
        provider record.
        """
        self.db_id = db_id

    def get_current_plot_data(self, database=DEFAULT_DATABASE,
                              schema=NIAMOTO_SCHEMA):
        """
        :return: A DataFrame containing the current database plot data for
        this provider.
        """
        connection = Connector.get_connection(
            database=database,
            schema=schema,
        )
        sel = select(plot).where(plot.c.provider_id == self.db_id)
        return pd.read_sql(sel, connection)
