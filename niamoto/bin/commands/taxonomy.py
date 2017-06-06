# coding: utf-8

import click

from niamoto.exceptions import DataSourceNotFoundError
from niamoto.decorators import cli_catch_unknown_error


@click.command('set_taxonomy')
@click.argument('csv_file_path')
@cli_catch_unknown_error
def set_taxonomy_cli(csv_file_path):
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
        m = "   Advice: run 'niamoto map_all_synonyms' to update occurrences " \
            "taxon identifiers"
        click.secho(m, fg='yellow')
    except DataSourceNotFoundError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)
