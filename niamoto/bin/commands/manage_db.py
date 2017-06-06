# coding: utf-8

import click

from niamoto.decorators import cli_catch_unknown_error


@click.command('init_db')
@cli_catch_unknown_error
def init_db_cli():
    """
    Initialize the Niamoto database.
    """
    from niamoto.api import manage_db as api_manage_db
    click.echo("Initializing Niamoto database...")
    api_manage_db.init_db()
    click.echo("Niamoto database had been successfully initialized!")
