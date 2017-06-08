# coding: utf-8

import click

from niamoto.decorators import cli_catch_unknown_error


@click.command("status")
@cli_catch_unknown_error
def get_general_status_cli():
    """
    Show the status of the Niamoto database.
    """
    from niamoto.api import status_api
    status = status_api.get_general_status()
    click.secho("    {} data providers are registered.".format(
        status['nb_data_providers']
    ))
    click.secho("    {} taxa are stored.".format(
        status['nb_taxa']
    ))
    click.secho("    {} taxon synonym keys are registered.".format(
        status['nb_synonym_keys']
    ))
    click.secho("    {} occurrences are stored.".format(
        status['nb_occurrences']
    ))
    click.secho("    {} plots are stored.".format(
        status['nb_plots']
    ))
    click.secho("    {} plots/occurrences are stored.".format(
        status['nb_plots_occurrences']
    ))
    click.secho("    {} rasters are stored.".format(
        status['nb_rasters']
    ))
    click.secho("    {} vectors are stored.".format(
        status['nb_vector'
               ''
               ''
               ''
               's']
    ))
