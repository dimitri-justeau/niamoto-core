# coding: utf-8

"""
API Module for managing data providers.
"""

from sqlalchemy import *
import pandas as pd

from niamoto.db.connector import Connector
from niamoto.db.metadata import data_provider_type, data_provider, \
    synonym_key_registry
from niamoto.db.utils import fix_db_sequences
from niamoto.data_providers.base_data_provider import BaseDataProvider
from niamoto.data_providers.provider_types import PROVIDER_TYPES


def get_data_provider_type_list():
    """
    :return: A Dataframe containing all the registered data provider types.
    """
    sel = select([data_provider_type])
    with Connector.get_connection() as connection:
        df = pd.read_sql(sel, connection, index_col='id')
    return df


def get_data_provider_list():
    """
    :return: A Dataframe containing all the registered data providers.
    """
    sel = select([
        data_provider.c.id,
        data_provider.c.name,
        data_provider_type.c.name.label('provider_type'),
        synonym_key_registry.c.name.label('synonym_key'),
        data_provider.c.date_create.label('date_create'),
        data_provider.c.date_update.label('date_update'),
        data_provider.c.last_sync.label('last_sync'),
    ]).select_from(
        data_provider.join(
            data_provider_type,
            data_provider.c.provider_type_id == data_provider_type.c.id
        ).outerjoin(
            synonym_key_registry,
            synonym_key_registry.c.id == data_provider.c.synonym_key_id
        )
    )
    with Connector.get_connection() as connection:
        df = pd.read_sql(sel, connection, index_col='id')
    return df


def add_data_provider(name, provider_type, *args, properties={},
                      synonym_key=None, return_object=False, **kwargs):
    """
    Register a data provider in a given Niamoto database.
    :param name: The name of the provider to register (must be unique).
    :param provider_type: The type of the provider to register. Must be a
    string value among:
        - 'PLANTNOTE': The Pl@ntnote data provider.
        - 'CSV': The CSV data provider.
    :param synonym_key: The synonym key for this provider.
    :param args: Additional args.
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
        properties=properties,
        synonym_key=synonym_key,
        return_object=return_object,
        **kwargs
    )


def delete_data_provider(name):
    """
    Register a data provider in a given Niamoto database.
    :param name: The name of the provider to register (must be unique).
    """
    BaseDataProvider.unregister_data_provider(name)
    fix_db_sequences()


def update_data_provider(current_name, new_name=None, properties={},
                         synonym_key=None):
    """
    Update an existing data provider.
    :param current_name:
    :param new_name:
    :param properties:
    :param synonym_key:
    :return:
    """
    provider = load_data_provider(current_name)
    provider.update_data_provider(
        current_name,
        new_name=new_name,
        properties=properties,
        synonym_key=synonym_key
    )


def sync_with_data_provider(name, *args, **kwargs):
    """
    Sync the Niamoto database with a data provider.
    :param name: The name of the data provider.
    :return: The sync report.
    """
    with Connector.get_connection() as connection:
        provider = load_data_provider(
            name,
            *args,
            connection=connection,
            **kwargs
        )
        sync_report = provider.sync()
    fix_db_sequences()
    return sync_report


def load_data_provider(name, *args, connection=None, **kwargs):
    BaseDataProvider.assert_data_provider_exists(name)
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
    # Look for args that must be set None
    none_values = [None, 'none', 'None', '0', 'n', 'N', ]
    nargs = [None if i in none_values else i for i in args]
    close_after = False
    if connection is None:
        close_after = True
        connection = Connector.get_engine().connect()
    r = connection.execute(sel)
    record = r.fetchone()
    name = record.name
    type_key = record.provider_type
    provider = PROVIDER_TYPES[type_key](name, *nargs, **kwargs)
    if close_after:
        connection.close()
    return provider
