# coding: utf-8

import click


@click.command('init_db')
def init_db_cli():
    """
    Initialize the Niamoto database.
    """
    from niamoto.api import manage_db as api_manage_db
    click.echo("Initializing Niamoto database...")
    try:
        api_manage_db.init_db()
        click.echo("Niamoto database had been successfully initialized!")
    except:
        click.secho(
            "An error occurred during database initialization.",
            fg='red'
        )
        click.get_current_context().exit(code=1)
