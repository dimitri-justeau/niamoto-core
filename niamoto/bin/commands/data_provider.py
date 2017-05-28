# coding: utf-8

import click

from niamoto.decorators import resolve_database


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
