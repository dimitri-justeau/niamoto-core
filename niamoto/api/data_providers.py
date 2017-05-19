# coding: utf-8

"""
API Module for managing data providers.
"""

from sqlalchemy import *
import pandas as pd

from niamoto.conf import settings
from niamoto.db.connector import Connector
from niamoto.db import metadata as niamoto_db_meta


def get_data_provider_type_list(database=settings.DEFAULT_DATABASE):
    """
    :param database The database to work with.
    :return: A Dataframe containing all the registered data provider types.
    """
    sel = select([niamoto_db_meta.data_provider_type])
    with Connector.get_connection(database=database) as connection:
        df = pd.read_sql(sel, connection, index_col='id')
    return df


def get_data_provider_list(database=settings.DEFAULT_DATABASE):
    """
    :param database The database to work with.
    :return: A Dataframe containing all the registered data providers.
    """
