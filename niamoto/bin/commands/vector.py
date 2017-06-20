# coding: utf-8

import click

from niamoto.bin.utils import format_datetime_to_date
from niamoto.exceptions import NoRecordFoundError, RecordAlreadyExistsError
from niamoto.decorators import cli_catch_unknown_error


@click.command("vectors")
@cli_catch_unknown_error
def list_vectors_cli():
    """
    List registered vectors.
    """
    from niamoto.api import vector_api
    vector_df = vector_api.get_vector_list()
    if len(vector_df) == 0:
        click.echo("Niamoto vector database is empty.")
        return
    click.echo(vector_df.to_string(
        formatters={
            'date_create': format_datetime_to_date,
            'date_update': format_datetime_to_date,
        }
    ))


@click.command('add_vector')
@click.argument('name')
@click.argument('vector_file_path')
@cli_catch_unknown_error
def add_vector_cli(name, vector_file_path):
    """
    Add a raster in Niamoto's vector database.
    """
    from niamoto.api import vector_api
    click.echo("Registering the vector in database...")
    try:
        vector_api.add_vector(
            vector_file_path,
            name,
        )
        click.echo("The vector had been successfully registered to the "
                   "Niamoto vector database!")
    except RecordAlreadyExistsError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command('update_vector')
@click.option(
    '--new_name',
    help='The new name of the vector',
    required=False
)
@click.argument('name')
@click.argument('vector_file_path')
@cli_catch_unknown_error
def update_vector_cli(name, vector_file_path, new_name=None):
    """
    Update an existing vector in Niamoto's vector database.
    """
    from niamoto.api import vector_api
    click.echo("Updating {} vector...".format(name))
    try:
        vector_api.update_vector(
            vector_file_path,
            name,
            new_name=new_name,
        )
        click.echo("The vector had been successfully updated!")
    except NoRecordFoundError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command('delete_vector')
@click.option('-y', default=False)
@click.argument('name')
@cli_catch_unknown_error
def delete_vector_cli(name, y=False):
    """
    Delete an existing vector from Niamoto's vector database.
    """
    if not y:
        m = "If you continue, the vector will be deleted from the " \
            "database, are you sure you want to continue?"
        if not click.confirm(m, default=True):
            click.secho("Operation aborted.")
            return
    from niamoto.api import vector_api
    click.echo("Deleting {} vector...".format(name))
    try:
        vector_api.delete_vector(name)
        click.echo("The vector had been successfully deleted!")
    except NoRecordFoundError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)
