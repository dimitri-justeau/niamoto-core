# coding: utf-8

import click

from niamoto.decorators import cli_catch_unknown_error
from niamoto.exceptions import BaseDataMartException


@click.command('dimension_types')
@cli_catch_unknown_error
def list_dimension_types_cli():
    """
    List the available dimension types.
    """
    from niamoto.api import data_marts_api
    try:
        types = data_marts_api.get_dimension_types()
        max_length = max([len(i) for i in types.keys()])
        for k, v in types.items():
            click.secho(
                '    {} :    {}'.format(
                    k.ljust(max_length),
                    v['description']
                )
            )
    except BaseDataMartException as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command('dimensions')
@cli_catch_unknown_error
def list_dimensions_cli():
    """
    List registered dimensions.
    """
    try:
        pass
    except BaseDataMartException as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command('fact_tables')
@cli_catch_unknown_error
def list_fact_tables():
    """
    List registered fact tables.
    """
    try:
        pass
    except BaseDataMartException as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)
