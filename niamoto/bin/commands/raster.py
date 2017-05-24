# coding: utf-8

import click


@click.command("rasters")
@click.option('--database', default=None)
def list_rasters_cli(database=None):
    """
    List registered rasters.
    """
    from niamoto.api import raster_api
    from niamoto.conf import settings
    if database is None:
        database = settings.DEFAULT_DATABASE
    try:
        raster_df = raster_api.get_raster_list(database)
        if len(raster_df) == 0:
            click.echo("Niamoto raster database is empty.")
            return
        click.echo(raster_df.to_string(
            formatters={
                'date_create': format_datetime_to_date,
                'date_update': format_datetime_to_date,
            }
        ))
    except:
        click.echo("An error occurred, please ensure that Niamoto is "
                   "properly configured.")
        click.get_current_context().exit(code=1)


def format_datetime_to_date(obj):
    return obj.strftime("%Y/%m/%d")


@click.command('add_raster')
@click.option(
    '--srid',
    default=None,
    help='SRID of the raster. If not specified, it will be detected '
         'automatically.'
)
@click.option('--database', default=None)
@click.argument('name')
@click.argument('tile_width')
@click.argument('tile_height')
@click.argument('raster_file_path')
def add_raster_cli(name, tile_width, tile_height, raster_file_path, srid=None,
                   database=None):
    """
    Add a raster in Niamoto's raster database.
    """
    from niamoto.conf import settings
    from niamoto.api import raster_api
    if database is None:
        database = settings.DEFAULT_DATABASE
    click.echo("Adding raster in database...")
    try:
        raster_api.add_raster(
            raster_file_path,
            name,
            tile_width,
            tile_height,
            srid=srid,
            database=database,
        )
        click.echo("The raster had been successfully added to the Niamoto "
                   "raster database!")
    except:
        click.echo("An error occurred while adding the raster.")
        click.get_current_context().exit(code=1)


@click.command('update_raster')
@click.option(
    '--srid',
    default=None,
    help='SRID of the raster. If not specified, it will be detected '
         'automatically.'
)
@click.option('--database', default=None)
@click.argument('name')
@click.argument('tile_width')
@click.argument('tile_height')
@click.argument('raster_file_path')
def update_raster_cli(name, tile_width, tile_height, raster_file_path,
                      srid=None, database=None):
    """
    Update an existing raster in Niamoto's raster database.
    """
    from niamoto.conf import settings
    from niamoto.api import raster_api
    if database is None:
        database = settings.DEFAULT_DATABASE
    click.echo("Updating {} raster...".format(name))
    try:
        raster_api.update_raster(
            raster_file_path,
            name,
            tile_width,
            tile_height,
            srid=srid,
            database=database,
        )
        click.echo("The raster had been successfully updated!")
    except:
        click.echo("An error occurred while updating the raster.")
        click.get_current_context().exit(code=1)


@click.command('delete_raster')
@click.option('--database', default=None)
@click.argument('name')
def delete_raster_cli(name, database=None):
    """
    Delete an existing raster from Niamoto's raster database.
    """
    from niamoto.conf import settings
    from niamoto.api import raster_api
    if database is None:
        database = settings.DEFAULT_DATABASE
    click.echo("Deleting {} raster...".format(name))
    try:
        raster_api.delete_raster(
            name,
            database=database,
        )
        click.echo("The raster had been successfully deleted!")
    except:
        click.echo("An error occurred while deleting the raster.")
        click.get_current_context().exit(code=1)
