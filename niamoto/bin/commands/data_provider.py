# coding: utf-8

import click

from niamoto.decorators import resolve_database
from niamoto.exceptions import NoRecordFoundError


@click.command("provider_types")
@click.option('--database', default=None)
@resolve_database
def list_data_provider_types(database=None):
    """
    List registered data provider types.
    """
    from niamoto.api.data_provider_api import get_data_provider_type_list
    try:
        provider_types_df = get_data_provider_type_list(database)
        if len(provider_types_df) == 0:
            click.echo(
                "There are no registered data provider types in the database."
            )
            return
        click.echo(provider_types_df.to_string())
    except:
        click.echo("An error occurred, please ensure that Niamoto is "
                   "properly configured.")
        click.get_current_context().exit(code=1)


@click.command("providers")
@click.option('--database', default=None)
@resolve_database
def list_data_providers(database=None):
    """
    List registered data providers.
    """
    from niamoto.api.data_provider_api import get_data_provider_list
    try:
        providers_df = get_data_provider_list(database)
        if len(providers_df) == 0:
            click.echo(
                "There are no registered data providers in the database."
            )
            return
        click.echo(providers_df.to_string())
    except:
        click.echo("An error occurred, please ensure that Niamoto is "
                   "properly configured.")
        click.get_current_context().exit(code=1)


@click.command("add_provider")
@click.argument("name")
@click.argument("provider_type")
@click.option('--database', default=None)
@resolve_database
def add_data_provider(name, provider_type, *args, database=None, **kwargs):
    """
    Register a data provider. The name of the data provider must be unique.
    The provider_type must be a value among: 'PLANTNOTE'.
    """
    from niamoto.api.data_provider_api import add_data_provider
    click.echo("Registering the data provider in database...")
    try:
        properties = {}
        if 'properties' in kwargs:
            properties = kwargs.pop('properties')
        add_data_provider(
            name,
            provider_type,
            *args,
            database=database,
            properties=properties,
            **kwargs
        )
        m = "The data provider had been successfully registered to Niamoto!"
        click.echo(m)
    except:
        m = "An error occurred while registering the data provider."
        click.echo(m)
        click.get_current_context().exit(code=1)


@click.command("delete_provider")
@click.argument("name")
@click.option('--database', default=None)
@resolve_database
def delete_data_provider(name, database=None):
    """
    Delete a data provider.
    """
    from niamoto.api.data_provider_api import delete_data_provider
    click.echo("Unregistering the data provider from the database...")
    try:
        delete_data_provider(
            name,
            database=database,
        )
        m = "The data provider had been successfully unregistered!"
        click.echo(m)
    except NoRecordFoundError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)
    except:
        m = "An error occurred while unregistering the data provider."
        click.echo(m, fg='red')
        click.get_current_context().exit(code=1)
