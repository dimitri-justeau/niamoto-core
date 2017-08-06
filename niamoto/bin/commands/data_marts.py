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


@click.command('delete_dimension')
@click.argument('dimension_name')
@cli_catch_unknown_error
def delete_dimension_cli(dimension_name):
    """
    Delete a registered dimension.
    """
    from niamoto.api import data_marts_api
    click.secho("Deleting the '{}' dimension...".format(dimension_name))
    data_marts_api.delete_dimension(dimension_name)
    click.secho(
        "The '{}' dimension had been successfully deleted!".format(
            dimension_name
        )
    )


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
        "Creating{} the '{}' vector dimension...".format(s1, vector_name)
    )
    data_marts_api.create_vector_dimension(
        vector_name,
        label_col=label_col,
        populate=populate
    )
    click.echo(
        "The '{}' vector dimension had been successfully created{}".format(
            vector_name,
            s2
        )
    )


@click.command('create_vector_hierarchy_dimension')
@click.argument('name')
@click.option(
    '--populate',
    help='Populate the dimension',
    is_flag=True,
)
@click.option(
    '--vector_dimension',
    '-vd',
    help="The vector dimension's names",
    type=str,
    multiple=True,
    required=True,
)
@cli_catch_unknown_error
def create_vector_hierarchy_dim_cli(name, vector_dimension, populate=True):
    """
    Create a vector hierarchy dimension from registered vector dimensions.
    """
    from niamoto.api import data_marts_api
    s1, s2 = '', '!'
    if populate:
        s1 = " and populating"
        s2 = " and populated!"
    click.echo(
        "Creating{} the '{}' vector hierarchy dimension...".format(s1, name)
    )
    data_marts_api.create_vector_hierarchy_dimension(
        name,
        vector_dimension,
        populate=populate
    )
    m = "The '{}' vector hierarchy dimension had been successfully created{}"
    click.echo(
        m.format(
            name,
            s2
        )
    )


@click.command('create_raster_dimension')
@click.option(
    '--populate',
    help='Populate the dimension',
    is_flag=True,
)
@click.option(
    '--cut_value',
    '-cv',
    help="The cut values",
    type=float,
    multiple=True,
)
@click.option(
    '--cut_label',
    '-cl',
    help="The cut labels",
    type=str,
    multiple=True,
)
@click.argument('raster_name')
@cli_catch_unknown_error
def create_raster_dim_cli(raster_name, cut_value=None, cut_label=None,
                          populate=True):
    """
    Create a raster dimension from a registered raster.
    """
    from niamoto.api import data_marts_api
    s1, s2 = '', '!'
    if populate:
        s1 = " and populating"
        s2 = " and populated!"
    click.echo(
        "Creating{} the '{}' raster dimension...".format(s1, raster_name)
    )
    cuts = (cut_value, cut_label)
    if len(cut_label) == 0:
        cuts = None
    data_marts_api.create_raster_dimension(
        raster_name,
        cuts=cuts,
        populate=populate
    )
    click.echo(
        "The '{}' raster dimension had been successfully created{}".format(
            raster_name,
            s2
        )
    )


@click.command('create_taxon_dimension')
@click.option(
    '--populate',
    help='Populate the dimension',
    is_flag=True,
)
@cli_catch_unknown_error
def create_taxon_dim_cli(populate=True):
    """
    Create the taxon dimension.
    """
    from niamoto.api import data_marts_api
    click.echo("Creating the taxon dimension...")
    data_marts_api.create_taxon_dimension(populate=populate)
    click.echo("The taxon dimension had been successfully created!")


@click.command('create_occurrence_location_dimension')
@click.option(
    '--populate',
    help='Populate the dimension',
    is_flag=True,
)
@cli_catch_unknown_error
def create_occurrence_location_dim_cli(populate=True):
    """
    Create the occurrence location dimension.
    """
    from niamoto.api import data_marts_api
    click.echo("Creating the occurrence location dimension...")
    data_marts_api.create_occurrence_location_dimension(populate=populate)
    click.echo(
        "The occurrence location dimension had been successfully created!"
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


@click.command('create_fact_table')
@click.option(
    '--dimension',
    '-d',
    help="The fact table's dimension names",
    type=str,
    multiple=True,
    required=True,
)
@click.option(
    '--measure',
    '-m',
    help="The fact table's measures names",
    type=str,
    multiple=True,
    required=True,
)
@click.argument('name')
@cli_catch_unknown_error
def create_fact_table_cli(name, dimension, measure):
    """
    Create and register a fact table from existing dimensions.
    Use -d <dimension_name> for each dimension, and -m <measure_name> for each
    measure.
    """
    from niamoto.api import data_marts_api
    click.echo(
        "Creating the '{}' fact table...".format(name)
    )
    data_marts_api.create_fact_table(
        name,
        dimension_names=dimension,
        measure_names=measure
    )
    click.echo(
        "The '{}' fact table had been successfully created!".format(name)
    )


@click.command('delete_fact_table')
@click.argument('fact_table_name')
@cli_catch_unknown_error
def delete_fact_table_cli(fact_table_name):
    """
    Delete a registered fact table.
    """
    from niamoto.api import data_marts_api
    click.secho("Deleting the '{}' fact table...".format(fact_table_name))
    data_marts_api.delete_fact_table(fact_table_name)
    click.secho(
        "The '{}' fact table had been successfully deleted!".format(
            fact_table_name
        )
    )


@click.command('populate_fact_table')
@click.argument('fact_table_name')
@click.argument('publisher_key')
@cli_catch_unknown_error
def populate_fact_table_cli(fact_table_name, publisher_key):
    """
    Populate a registered fact table using an available publisher.
    """
    from niamoto.api import data_marts_api
    click.secho(
        "Populating the '{}' fact table using the '{}' publisher...".format(
            fact_table_name,
            publisher_key
        )
    )
    data_marts_api.populate_fact_table(fact_table_name, publisher_key)
    click.secho(
        "The '{}' fact table had been successfully populated!".format(
            fact_table_name
        )
    )
