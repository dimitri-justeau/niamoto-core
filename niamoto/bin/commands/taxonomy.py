# coding: utf-8

import click

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


@click.command('map_all_synonyms')
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


@click.command("synonym_keys")
@cli_catch_unknown_error
def get_synonym_keys_cli():
    """
    List the registered synonym keys.
    """
    from niamoto.bin.utils import format_datetime_to_date
    from niamoto.api.taxonomy_api import get_synonym_keys
    synonym_keys = get_synonym_keys()
    click.echo(synonym_keys.to_string(
        formatters={
            'date_create': format_datetime_to_date,
            'date_update': format_datetime_to_date,
        }
    ))
