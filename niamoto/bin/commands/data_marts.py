# coding: utf-8

import click

from niamoto.decorators import cli_catch_unknown_error


@click.command('dimension_types')
@cli_catch_unknown_error
def list_dimension_types_cli():
    """
    List the available dimension types.
    """
    from niamoto.api import data_marts_api
    types = data_marts_api.get_dimension_types()
    max_length = max([len(i) for i in types.keys()])
    for k, v in types.items():
        click.secho(
            '    {} :    {}'.format(
                k.ljust(max_length),
                v['description']
            )
        )


@click.command('dimensions')
@cli_catch_unknown_error
def list_dimensions_cli():
    """
    List registered dimensions.
    """
    from niamoto.bin.utils import format_datetime_to_date
    from niamoto.api import data_marts_api
    dimensions_df = data_marts_api.get_registered_dimensions()
    if len(dimensions_df) == 0:
        click.secho("There are no registered dimensions in database.")
        return
    click.secho(dimensions_df.to_string(
        formatters={
            'date_create': format_datetime_to_date,
            'date_update': format_datetime_to_date,
        }
    ))


@click.command('fact_tables')
@cli_catch_unknown_error
def list_fact_tables_cli():
    """
    List registered fact tables.
    """
    from niamoto.bin.utils import format_datetime_to_date
    from niamoto.api import data_marts_api
    fact_tables_df = data_marts_api.get_registered_fact_tables()
    if len(fact_tables_df) == 0:
        click.secho(
            "There are no registered fact tables in database."
        )
        return
    click.secho(fact_tables_df.to_string(
        formatters={
            'date_create': format_datetime_to_date,
            'date_update': format_datetime_to_date,
        }
    ))
