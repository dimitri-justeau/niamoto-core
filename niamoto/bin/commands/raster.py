# coding: utf-8

import click

from niamoto.decorators import cli_catch_unknown_error


@click.command("rasters")
@cli_catch_unknown_error
def list_rasters_cli():
    """
    List the registered rasters.
    """
    from niamoto.bin.utils import format_datetime_to_date
    from niamoto.api import raster_api
    raster_df = raster_api.get_raster_list()
    if len(raster_df) == 0:
        click.echo("Niamoto raster database is empty.")
        return
    click.echo(raster_df.to_string(
        formatters={
            'date_create': format_datetime_to_date,
            'date_update': format_datetime_to_date,
        }
    ))


@click.command('add_raster')
@click.option(
    '--tile_dimension',
    '-t',
    help='Tile dimension <width>x<height>',
    required=False
)
@click.option(
    '--register',
    '-R',
    help='Register the raster as a filesystem (out-db) raster. '
         '(-R option of raster2pgsql).',
    is_flag=True,
    default=False
)
@click.argument('name')
@click.argument('raster_file_path')
@cli_catch_unknown_error
def add_raster_cli(name, raster_file_path, tile_dimension=None,
                   register=False):
    """
    Add a raster in Niamoto's raster database.
    """
    from niamoto.api import raster_api
    click.echo("Registering the raster in database...")
    if tile_dimension is not None:
        tile_dimension = [int(i) for i in tile_dimension.split('x')]
    raster_api.add_raster(
        raster_file_path,
        name,
        tile_dimension=tile_dimension,
        register=register
    )
    click.echo("The raster had been successfully registered to the Niamoto"
               " raster database!")


@click.command('update_raster')
@click.option(
    '--tile_dimension',
    '-t',
    help='Tile dimension <width>x<height>',
    required=False
)
@click.option(
    '--new_name',
    help='The new name of the raster',
    required=False
)
@click.option(
    '--register',
    '-R',
    help='Register the raster as a filesystem (out-db) raster. '
         '(-R option of raster2pgsql).',
    is_flag=True,
    default=False
)
@click.argument('name')
@click.argument('raster_file_path')
@cli_catch_unknown_error
def update_raster_cli(name, raster_file_path, new_name=None,
                      tile_dimension=None, register=False):
    """
    Update an existing raster in Niamoto's raster database.
    """
    from niamoto.api import raster_api
    click.echo("Updating {} raster...".format(name))
    if tile_dimension is not None:
        tile_dimension = [int(i) for i in tile_dimension.split('x')]
    raster_api.update_raster(
        raster_file_path,
        name,
        new_name=new_name,
        tile_dimension=tile_dimension,
        register=register,
    )
    click.echo("The raster had been successfully updated!")


@click.command('delete_raster')
@click.option('-y', default=False)
@click.argument('name')
@cli_catch_unknown_error
def delete_raster_cli(name, y=False):
    """
    Delete an existing raster from Niamoto's raster database.
    """
    if not y:
        m = "If you continue, the raster will be deleted from the " \
            "database, are you sure you want to continue?"
        if not click.confirm(m, default=True):
            click.secho("Operation aborted.")
            return
    from niamoto.api import raster_api
    click.echo("Deleting {} raster...".format(name))
    raster_api.delete_raster(name)
    click.echo("The raster had been successfully deleted!")


@click.command('raster_to_occurrences')
@click.argument('raster_name')
@cli_catch_unknown_error
def extract_raster_values_to_occurrences_cli(raster_name):
    """
    Extract raster values to occurrences properties.
    """
    from niamoto.api import raster_api
    click.secho("Extracting '{}' raster values to occurrences...".format(
        raster_name
    ))
    raster_api.extract_raster_values_to_occurrences(raster_name)
    click.echo("The raster values had been successfully extracted!")


@click.command('raster_to_plots')
@click.argument('raster_name')
@cli_catch_unknown_error
def extract_raster_values_to_plots_cli(raster_name):
    """
    Extract raster values to plots properties.
    """
    from niamoto.api import raster_api
    click.secho("Extracting '{}' raster values to plots...".format(
        raster_name
    ))
    raster_api.extract_raster_values_to_plots(raster_name)
    click.echo("The raster values had been successfully extracted!")


@click.command('all_rasters_to_occurrences')
@cli_catch_unknown_error
def extract_all_rasters_values_to_occurrences_cli():
    """
    Extract raster values to occurrences properties for all registered rasters.
    """
    from niamoto.api import raster_api
    click.secho("Extracting all rasters values to occurrences...")
    raster_api.extract_all_rasters_values_to_occurrences()
    click.echo("The rasters values had been successfully extracted!")


@click.command('all_rasters_to_plots')
@cli_catch_unknown_error
def extract_all_rasters_values_to_plots_cli():
    """
    Extract raster values to plots properties for all registered rasters.
    """
    from niamoto.api import raster_api
    click.secho("Extracting all rasters values to plots...")
    raster_api.extract_all_rasters_values_to_plots()
    click.echo("The rasters values had been successfully extracted!")
