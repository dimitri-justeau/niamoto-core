# coding: utf-8

import click

from niamoto.exceptions import DataSourceNotFoundError
from niamoto.decorators import cli_catch_unknown_error


@click.command('set_taxonomy')
@click.argument('csv_file_path')
@click.option('--no_mapping', type=bool, default=False)
@cli_catch_unknown_error
def set_taxonomy_cli(csv_file_path, no_mapping=False):
    """
    Set the taxonomy.
    """
    from niamoto.api import taxonomy_api
    click.secho("Setting the taxonomy...")
    try:
        nb, synonyms = taxonomy_api.set_taxonomy(csv_file_path)
        click.secho("The taxonomy had been successfully set!")
        click.secho("    {} taxa inserted".format(nb), fg='green')
        click.secho(
            "    {} synonyms inserted: {}".format(len(synonyms), synonyms),
            fg='green',
        )
        if no_mapping:
            m = "   Advice: run 'niamoto map_all_synonyms' " \
                "to update occurrences taxon identifiers"
            click.secho(m, fg='yellow')
        else:
            map_all_synonyms_cli.invoke(click.Context(map_all_synonyms_cli))
    except DataSourceNotFoundError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command('map_all_synonym')
@cli_catch_unknown_error
def map_all_synonyms_cli():
    """
    Update the synonym mapping for every data provider registered in the
    database.
    """
    from niamoto.api import taxonomy_api
    click.secho("Mapping all synonyms...")
    mapped = taxonomy_api.map_all_synonyms()
    if len(mapped) == 0:
        m = "No mapping had been processed since there are no registered " \
            "data providers in the database."
        click.secho(m)
    else:
        click.secho("All synonyms had been mapped!")
