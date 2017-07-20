# coding: utf-8

import os

import click

from niamoto.bin.commands.raster import list_rasters_cli, add_raster_cli, \
    update_raster_cli, delete_raster_cli, \
    extract_raster_values_to_occurrences_cli, \
    extract_raster_values_to_plots_cli, \
    extract_all_rasters_values_to_occurrences_cli, \
    extract_all_rasters_values_to_plots_cli
from niamoto.bin.commands.vector import list_vectors_cli, add_vector_cli, \
    update_vector_cli, delete_vector_cli
from niamoto.bin.commands.manage_db import init_db_cli
from niamoto.bin.commands.init_niamoto_home import init_niamoto_home_cli
from niamoto.bin.commands.data_provider import list_data_provider_types, \
    list_data_providers, add_data_provider, delete_data_provider, sync, \
    update_data_provider_cli
from niamoto.bin.commands.taxonomy import set_taxonomy_cli, \
    map_all_synonyms_cli, get_synonym_keys_cli
from niamoto.bin.commands.status import get_general_status_cli
from niamoto.bin.commands.publish import publish_cli, list_publishers_cli, \
    list_publish_formats_cli, init_publish_cli
from niamoto.bin.commands.data_marts import list_dimension_types_cli, \
    list_dimensions_cli, list_fact_tables_cli

from niamoto import conf
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


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
    try:
        conf.set_niamoto_home()
        conf.set_settings()
        if context.invoked_subcommand == 'publish':
            init_publish_cli()
    except Exception as err:
        LOGGER.debug(str(err))

# General commands
niamoto_cli.add_command(init_niamoto_home_cli)
niamoto_cli.add_command(init_db_cli)
niamoto_cli.add_command(get_general_status_cli)

# Raster commands
niamoto_cli.add_command(list_rasters_cli)
niamoto_cli.add_command(add_raster_cli)
niamoto_cli.add_command(update_raster_cli)
niamoto_cli.add_command(delete_raster_cli)
niamoto_cli.add_command(extract_raster_values_to_occurrences_cli)
niamoto_cli.add_command(extract_raster_values_to_plots_cli)
niamoto_cli.add_command(extract_all_rasters_values_to_occurrences_cli)
niamoto_cli.add_command(extract_all_rasters_values_to_plots_cli)

# Vector commands
niamoto_cli.add_command(list_vectors_cli)
niamoto_cli.add_command(add_vector_cli)
niamoto_cli.add_command(update_vector_cli)
niamoto_cli.add_command(delete_vector_cli)

# Data provider commands
niamoto_cli.add_command(list_data_provider_types)
niamoto_cli.add_command(list_data_providers)
niamoto_cli.add_command(add_data_provider)
niamoto_cli.add_command(delete_data_provider)
niamoto_cli.add_command(update_data_provider_cli)
niamoto_cli.add_command(sync)

# Taxonomy commands
niamoto_cli.add_command(set_taxonomy_cli)
niamoto_cli.add_command(map_all_synonyms_cli)
niamoto_cli.add_command(get_synonym_keys_cli)

# Data publisher commands
niamoto_cli.add_command(publish_cli)
niamoto_cli.add_command(list_publishers_cli)
niamoto_cli.add_command(list_publish_formats_cli)

# Data marts commands
niamoto_cli.add_command(list_dimension_types_cli)
niamoto_cli.add_command(list_dimensions_cli)
niamoto_cli.add_command(list_fact_tables_cli)


if __name__ == '__main__':
    niamoto_cli(params={})
