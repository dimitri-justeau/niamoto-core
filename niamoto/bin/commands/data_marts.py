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
    List the registered dimensions.
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


@click.command('create_vector_dimension')
@click.option(
    '--label_col',
    help='The label column name of the dimension',
    default='label',
    type=str,
)
@click.option(
    '--populate',
    help='Populate the dimension',
    is_flag=True,
)
@click.argument('vector_name')
@cli_catch_unknown_error
def create_vector_dim_cli(vector_name, label_col='label', populate=True):
    """
    Create a vector dimension from a registered vector.
    """
    from niamoto.api import data_marts_api
    s1, s2 = '', '!'
    if populate:
        s1 = " and populating"
        s2 = " and populated!"
    click.echo(
        "Creating{} the {} vector dimension...".format(s1, vector_name)
    )
    data_marts_api.create_vector_dimension(
        vector_name,
        label_col=label_col,
        populate=populate
    )
    click.echo(
        "The {} vector dimension had been successfully created{}".format(
            vector_name,
            s2
        )
    )


@click.command('fact_tables')
@cli_catch_unknown_error
def list_fact_tables_cli():
    """
    List the registered fact tables.
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
