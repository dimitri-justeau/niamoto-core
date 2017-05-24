# coding: utf-8

import click


@click.command('init_db')
@click.option('--database', default=None)
def init_db_cli(database=None):
    """
    Initialize the Niamoto database.
    """
    from niamoto.conf import settings
    from niamoto.api import init_db as api_init_db
    if database is None:
        database = settings.DEFAULT_DATABASE
    click.echo("Initializing Niamoto database...")
    try:
        api_init_db.init_db(database=database)
        click.echo("Niamoto database had been successfully initialized!")
    except:
        click.echo("An error occurred during database initialization.")
        click.get_current_context().exit(code=1)
