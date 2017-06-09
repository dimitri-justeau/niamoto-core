# coding: utf-8

import os

import click

from niamoto.bin.commands.raster import list_rasters_cli, add_raster_cli, \
    update_raster_cli, delete_raster_cli
from niamoto.bin.commands.manage_db import init_db_cli
from niamoto.bin.commands.init_niamoto_home import init_niamoto_home_cli
from niamoto.bin.commands.data_provider import list_data_provider_types, \
    list_data_providers, add_data_provider, delete_data_provider, sync
from niamoto.bin.commands.taxonomy import set_taxonomy_cli, \
    map_all_synonyms_cli, get_synonym_keys_cli
from niamoto.bin.commands.status import get_general_status_cli
from niamoto.bin.commands.publish import publish_cli
from niamoto import conf


@click.group()
@click.option(
    '--niamoto_home',
    default=conf.DEFAULT_NIAMOTO_HOME,
    help='Niamoto home path (default: ~/niamoto).'
)
@click.pass_context
def niamoto_cli(context, niamoto_home=conf.DEFAULT_NIAMOTO_HOME):
    """
    Niamoto command line interface.
    """
    context.params['niamoto_home'] = niamoto_home
    os.environ['NIAMOTO_HOME'] = niamoto_home
    conf.set_niamoto_home()
    conf.set_settings()


niamoto_cli.add_command(init_niamoto_home_cli)
niamoto_cli.add_command(init_db_cli)
niamoto_cli.add_command(list_rasters_cli)
niamoto_cli.add_command(add_raster_cli)
niamoto_cli.add_command(update_raster_cli)
niamoto_cli.add_command(delete_raster_cli)
niamoto_cli.add_command(list_data_provider_types)
niamoto_cli.add_command(list_data_providers)
niamoto_cli.add_command(add_data_provider)
niamoto_cli.add_command(delete_data_provider)
niamoto_cli.add_command(sync)
niamoto_cli.add_command(set_taxonomy_cli)
niamoto_cli.add_command(map_all_synonyms_cli)
niamoto_cli.add_command(get_synonym_keys_cli)
niamoto_cli.add_command(get_general_status_cli)
niamoto_cli.add_command(publish_cli)


if __name__ == '__main__':
    niamoto_cli(params={})
