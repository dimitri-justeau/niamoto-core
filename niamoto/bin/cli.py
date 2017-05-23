# coding: utf-8

import os

import click

from niamoto.bin.commands.raster import list_rasters, add_raster, \
    update_raster, delete_raster
from niamoto.bin.commands.init_db import init_db


@click.group()
@click.option(
    '--niamoto_home',
    default=None,
    help='Niamoto home path (default: ~/niamoto).'
)
@click.pass_context
def niamoto_cli(context, niamoto_home=None):
    """
    Niamoto command line interface.
    """
    if niamoto_home is None:
        from niamoto.conf import DEFAULT_NIAMOTO_HOME
        niamoto_home = DEFAULT_NIAMOTO_HOME
    context.params['niamoto_home'] = niamoto_home
    os.environ['NIAMOTO_HOME'] = niamoto_home


niamoto_cli.add_command(list_rasters)
niamoto_cli.add_command(add_raster)
niamoto_cli.add_command(update_raster)
niamoto_cli.add_command(delete_raster)
niamoto_cli.add_command(init_db)


if __name__ == '__main__':
    niamoto_cli(params={})
