# coding: utf-8

import click

from niamoto.decorators import resolve_database


@click.command('init_db')
@click.option('--database', default=None)
@resolve_database
def init_db_cli(database=None):
    """
    Initialize the Niamoto database.
    """
    from niamoto.api import init_db as api_init_db
    click.echo("Initializing Niamoto database...")
    try:
        api_init_db.init_db(database=database)
        click.echo("Niamoto database had been successfully initialized!")
    except:
        click.echo("An error occurred during database initialization.")
        click.get_current_context().exit(code=1)
