# coding: utf-8

import click


@click.group()
def raster():
    """
    Raster commands.
    """
    pass


@raster.command("list")
def list_rasters():
    """
    List registered rasters.
    """
    from niamoto.conf import settings
    from niamoto.api import raster_api
    raster_df = raster_api.get_raster_list(settings.DEFAULT_DATABASE)
    if len(raster_df) == 0:
        click.echo("Niamoto's raster database is empty.")
        return
    click.echo(raster_df.to_string(
        formatters={
            'date_create': format_datetime_to_date,
            'date_update': format_datetime_to_date,
        }
    ))


def format_datetime_to_date(obj):
    return obj.strftime("%Y/%m/%d")


@raster.command('add')
@click.option(
    '--srid',
    default=None,
    help='SRID of the raster. If not specified, it will be detected '
         'automatically.'
)
@click.argument('name')
@click.argument('tile_width')
@click.argument('tile_height')
@click.argument('raster_file_path')
def add_raster(name, tile_width, tile_height, raster_file_path, srid=None):
    """
    Add a raster in Niamoto's raster database.
    """
    from niamoto.conf import settings
    from niamoto.api import raster_api
    click.echo("Adding raster in database...")
    try:
        raster_api.add_raster(
            raster_file_path,
            name,
            tile_width,
            tile_height,
            srid=srid,
            database=settings.DEFAULT_DATABASE,
        )
        click.echo("The raster had been successfully added to the Niamoto "
                   "raster database!")
    except:
        click.echo("An error occurred while adding the raster.")
        click.get_current_context().exit(code=1)


@raster.command('update')
@click.option(
    '--srid',
    default=None,
    help='SRID of the raster. If not specified, it will be detected '
         'automatically.'
)
@click.argument('name')
@click.argument('tile_width')
@click.argument('tile_height')
@click.argument('raster_file_path')
def update_raster(name, tile_width, tile_height, raster_file_path, srid=None):
    """
    Update an existing raster in Niamoto's raster database.
    """
    from niamoto.conf import settings
    from niamoto.api import raster_api
    click.echo("Updating {} raster...".format(name))
    try:
        raster_api.update_raster(
            raster_file_path,
            name,
            tile_width,
            tile_height,
            srid=srid,
            database=settings.DEFAULT_DATABASE,
        )
        click.echo("The raster had been successfully updated!")
    except:
        click.echo("An error occurred while updating the raster.")
        click.get_current_context().exit(code=1)


@raster.command('delete')
@click.argument('name')
def delete_raster(name):
    """
    Delete an existing raster from Niamoto's raster database.
    """
    from niamoto.conf import settings
    from niamoto.api import raster_api
    click.echo("Deleting {} raster...".format(name))
    try:
        raster_api.delete_raster(
            name,
            database=settings.DEFAULT_DATABASE,
        )
        click.echo("The raster had been successfully deleted!")
    except:
        click.echo("An error occurred while deleting the raster.")
        click.get_current_context().exit(code=1)
