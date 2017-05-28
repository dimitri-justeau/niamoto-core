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
    from niamoto.api import data_provider_api
