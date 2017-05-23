# coding: utf-8

import click


@click.command('init_db')
def init_db():
    """
    Initialize the Niamoto database.
    """
    from niamoto.api import init_db
    from niamoto.conf import settings
    click.echo("Initializing Niamoto database...")
    try:
        init_db.init_db(database=settings.DEFAULT_DATABASE)
        click.echo("Niamoto database had been successfully initialized!")
    except:
        click.echo("An error occurred during database initialization.")
        click.get_current_context().exit(code=1)
