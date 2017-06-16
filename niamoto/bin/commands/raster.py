# coding: utf-8

import click

from niamoto.bin.utils import format_datetime_to_date
from niamoto.exceptions import NoRecordFoundError, RecordAlreadyExistsError
from niamoto.decorators import cli_catch_unknown_error


@click.command("rasters")
@cli_catch_unknown_error
def list_rasters_cli():
    """
    List registered rasters.
    """
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
    '--srid',
    default=None,
    help='SRID of the raster. If not specified, it will be detected '
         'automatically.'
)
@click.option(
    '--tile_dimension',
    '-t',
    help='Tile dimension <width>x<height>',
    required=False
)
@click.argument('name')
@click.argument('raster_file_path')
@cli_catch_unknown_error
def add_raster_cli(name, raster_file_path, tile_dimension=None, srid=None):
    """
    Add a raster in Niamoto's raster database.
    """
    from niamoto.api import raster_api
    click.echo("Registering the raster in database...")
    if tile_dimension is not None:
        tile_dimension = [int(i) for i in tile_dimension.split('x')]
    try:
        raster_api.add_raster(
            raster_file_path,
            name,
            tile_dimension=tile_dimension,
            srid=srid,
        )
        click.echo("The raster had been successfully registered to the Niamoto"
                   " raster database!")
    except RecordAlreadyExistsError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command('update_raster')
@click.option(
    '--srid',
    default=None,
    help='SRID of the raster. If not specified, it will be detected '
         'automatically.'
)
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
@click.argument('name')
@click.argument('raster_file_path')
@cli_catch_unknown_error
def update_raster_cli(name, raster_file_path, new_name=None,
                      tile_dimension=None, srid=None):
    """
    Update an existing raster in Niamoto's raster database.
    """
    from niamoto.api import raster_api
    click.echo("Updating {} raster...".format(name))
    if tile_dimension is not None:
        tile_dimension = [int(i) for i in tile_dimension.split('x')]
    try:
        raster_api.update_raster(
            raster_file_path,
            name,
            new_name=new_name,
            tile_dimension=tile_dimension,
            srid=srid,
        )
        click.echo("The raster had been successfully updated!")
    except NoRecordFoundError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


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
    try:
        raster_api.delete_raster(name)
        click.echo("The raster had been successfully deleted!")
    except NoRecordFoundError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)
