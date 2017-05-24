# coding: utf-8

import os

import click

from niamoto.constants import DEFAULT_NIAMOTO_HOME, HOME_ENVIRONMENT_VARIABLE


@click.command('init_niamoto_home')
@click.option('--niamoto_home_path', default=None)
def init_niamoto_home_cli(niamoto_home_path=None):
    """
    Initialize the Niamoto home directory.
    """
    if niamoto_home_path is None:
        if HOME_ENVIRONMENT_VARIABLE in os.environ:
            niamoto_home_path = os.environ[HOME_ENVIRONMENT_VARIABLE]
        else:
            niamoto_home_path = DEFAULT_NIAMOTO_HOME
    from niamoto.api import init_niamoto_home as api_init_niamoto_home
    click.echo("Initializing Niamoto home directory...")
    try:
        api_init_niamoto_home.init_niamoto_home(niamoto_home_path)
        click.echo("Niamoto home directory had been successfully initialized!")
    except:
        click.echo("An error occurred during home directory initialization.")
        click.get_current_context().exit(code=1)
