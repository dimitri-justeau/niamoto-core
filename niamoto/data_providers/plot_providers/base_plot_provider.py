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

    def __init__(self, data_provider):
        """
        :param data_provider: The parent data provider.
        """
        self.data_provider = data_provider

    def get_niamoto_plot_dataframe(self, database=DEFAULT_DATABASE,
                                   schema=NIAMOTO_SCHEMA):
        """
        :return: A DataFrame containing the plot data for this
        provider that is currently stored in the Niamoto database.
        """
        with Connector.get_connection(
            database=database,
            schema=schema,
        ) as connection:
            sel = select([plot]).where(
                plot.c.provider_id == self.data_provider.db_id
            )
            return pd.read_sql(sel, connection)

    def get_provider_occurrence_dataframe(self):
        """
        :return: A DataFrame containing the plot data currently
        available from the provider. The index of the DataFrame corresponds
        to the provider's pk.
        """
        raise NotImplementedError()
