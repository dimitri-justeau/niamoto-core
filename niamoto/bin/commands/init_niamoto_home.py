# coding: utf-8

import click

from niamoto.constants import DEFAULT_NIAMOTO_HOME


@click.command('init_niamoto_home')
@click.option('--niamoto_home_path', default=DEFAULT_NIAMOTO_HOME)
def init_niamoto_home_cli(niamoto_home_path=DEFAULT_NIAMOTO_HOME):
    """
    Initialize the Niamoto home directory.
    """
    from niamoto.api import init_niamoto_home as api_init_niamoto_home
    click.echo("Initializing Niamoto home directory...")
    try:
        api_init_niamoto_home.init_niamoto_home(niamoto_home_path)
        click.echo("Niamoto home directory had been successfully initialized!")
    except:
        click.echo("An error occurred during home directory initialization.")
        click.get_current_context().exit(code=1)
