# coding: utf-8

from sqlalchemy.sql import select
import pandas as pd

from niamoto.db.metadata import plot


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

    def get_current_plot_data(self):
        """
        :return: A DataFrame containing the current database plot data for
        this provider.
        """
        sel = select(plot).where(plot.c.provider_id == self.db_id)
        return pd.read_sql(sel)
