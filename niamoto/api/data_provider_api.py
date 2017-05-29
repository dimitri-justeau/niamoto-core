# coding: utf-8

"""
API Module for managing data providers.
"""

from sqlalchemy import *
import pandas as pd

from niamoto.conf import settings
from niamoto.db.connector import Connector
from niamoto.db.metadata import data_provider_type, data_provider
from niamoto.data_providers.base_data_provider import BaseDataProvider
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider


PROVIDER_TYPES = {
    PlantnoteDataProvider.get_type_name(): PlantnoteDataProvider,
}


def get_data_provider_type_list(database=settings.DEFAULT_DATABASE):
    """
    :param database The database to work with.
    :return: A Dataframe containing all the registered data provider types.
    """
    sel = select([data_provider_type])
    with Connector.get_connection(database=database) as connection:
        df = pd.read_sql(sel, connection, index_col='id')
    return df


def get_data_provider_list(database=settings.DEFAULT_DATABASE):
    """
    :param database The database to work with.
    :return: A Dataframe containing all the registered data providers.
    """
    sel = select([
        data_provider.c.id,
        data_provider.c.name,
        data_provider_type.c.name.label('provider_type'),
    ]).select_from(
        data_provider.join(
            data_provider_type,
            data_provider.c.provider_type_id == data_provider_type.c.id
        )
    )
    with Connector.get_connection(database=database) as connection:
        df = pd.read_sql(sel, connection, index_col='id')
    return df


def add_data_provider(name, provider_type, *args,
                      database=settings.DEFAULT_DATABASE,
                      properties={}, return_object=False, **kwargs):
    """
    Register a data provider in a given Niamoto database.
    :param name: The name of the provider to register (must be unique).
    :param provider_type: The type of the provider to register. Must be a
    string value among:
        - 'PLANTNOTE': The Pl@ntnote data provider.
    :param args: Additional args.
    :param database: The database to work with.
    :param properties: Properties dict to store for the data provider.
    :param return_object: If True, return the created object.
    :param kwargs: Additional keyword args.
    """
    if provider_type not in PROVIDER_TYPES:
        m = "The provider type '{}' does not exist. Please use one of the " \
            "following values: {}."
        e = ValueError(m.format(
            provider_type,
            ', '.join(PROVIDER_TYPES.keys())
        ))
        raise e
    provider_cls = PROVIDER_TYPES[provider_type]
    return provider_cls.register_data_provider(
        name,
        *args,
        database=database,
        properties=properties,
        return_object=return_object,
        **kwargs
    )


def delete_data_provider(name, database=settings.DEFAULT_DATABASE):
    """
    Register a data provider in a given Niamoto database.
    :param name: The name of the provider to register (must be unique).
    :param database: The database to work with.
    """
    BaseDataProvider.unregister_data_provider(name, database=database)


def sync_with_data_provider(name, *args, database=settings.DEFAULT_DATABASE,
                            **kwargs):
    """
    Sync the Niamoto database with a data provider.
    :param name: The name of the data provider.
    :param database: The database to work with.
    :return: The sync report.
    """
    BaseDataProvider.assert_data_provider_exists(name, database)
    sel = select([
        data_provider.c.id,
        data_provider.c.name,
        data_provider_type.c.name.label('provider_type'),
    ]).select_from(
        data_provider.join(
            data_provider_type,
            data_provider.c.provider_type_id == data_provider_type.c.id
        )
    ).where(
        data_provider.c.name == name
    )
    with Connector.get_connection(database=database) as connection:
        r = connection.execute(sel)
        record = r.fetchone()
        name = record.name
        type_key = record.provider_type
        provider = PROVIDER_TYPES[type_key](
            name, *args,
            database=database, **kwargs
        )
        return provider.sync()
