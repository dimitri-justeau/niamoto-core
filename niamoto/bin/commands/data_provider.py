# coding: utf-8

import click

from niamoto.bin.utils import format_datetime_to_date
from niamoto.exceptions import NoRecordFoundError, RecordAlreadyExistsError, \
    BaseDataProviderException, NiamotoException
from niamoto.decorators import cli_catch_unknown_error


@click.command("provider_types")
@cli_catch_unknown_error
def list_data_provider_types():
    """
    List registered data provider types.
    """
    from niamoto.api.data_provider_api import get_data_provider_type_list
    provider_types_df = get_data_provider_type_list()
    if len(provider_types_df) == 0:
        click.echo(
            "There are no registered data provider types in the database."
        )
        return
    click.echo(provider_types_df.to_string())


@click.command("providers")
@cli_catch_unknown_error
def list_data_providers():
    """
    List registered data providers.
    """
    from niamoto.api.data_provider_api import get_data_provider_list
    providers_df = get_data_provider_list()
    if len(providers_df) == 0:
        click.echo(
            "There are no registered data providers in the database."
        )
        return
    click.echo(providers_df.to_string(
        formatters={
            'date_create': format_datetime_to_date,
            'date_update': format_datetime_to_date,
            'last_sync': format_datetime_to_date,
        }
    ))


@click.command("add_provider")
@click.argument("name")
@click.argument("provider_type")
@click.argument('synonym_key', required=False, default=None)
@cli_catch_unknown_error
def add_data_provider(name, provider_type, synonym_key=None, *args, **kwargs):
    """
    Register a data provider. The name of the data provider must be unique.
    The available provider types can be obtained using the
    'niamoto provider_types' command. The available synonym keys can be
    obtained using the 'niamoto synonym_keys" command.
    """
    from niamoto.api.data_provider_api import add_data_provider
    click.echo("Registering the data provider in database...")
    try:
        properties = {}
        if 'properties' in kwargs:
            properties = kwargs.pop('properties')
        add_data_provider(
            name,
            provider_type,
            *args,
            properties=properties,
            synonym_key=synonym_key,
            **kwargs
        )
        m = "The data provider had been successfully registered to Niamoto!"
        click.echo(m)
    except (RecordAlreadyExistsError, NoRecordFoundError) as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command("delete_provider")
@click.argument("name")
@click.option('-y', default=False)
@cli_catch_unknown_error
def delete_data_provider(name, y=False):
    """
    Delete a data provider.
    """
    if not y:
        m = "If you delete the data provider, all the data referring to it" \
            " will also be deleted, do you want to continue?"
        if not click.confirm(m, default=True):
            click.secho("Operation aborted.")
            return
    from niamoto.api.data_provider_api import delete_data_provider
    click.echo("Unregistering the data provider from the database...")
    try:
        delete_data_provider(name)
        m = "The data provider had been successfully unregistered!"
        click.echo(m)
    except NoRecordFoundError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command("update_provider")
@click.argument("current_name")
@click.option('--new_name', default=None)
@click.option('--synonym_key', default=None)
@cli_catch_unknown_error
def update_data_provider_cli(current_name, new_name=None, synonym_key=None):
    """
    Update a data provider.
    """
    from niamoto.api.data_provider_api import update_data_provider
    click.echo("Updating the data provider '{}'...".format(current_name))
    try:
        update_data_provider(
            current_name,
            new_name=new_name,
            synonym_key=synonym_key,
        )
        m = "The data provider had been successfully updated!"
        click.echo(m)
    except NiamotoException as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command("sync")
@click.argument("provider_name")
@click.argument('provider_args', nargs=-1, type=click.UNPROCESSED)
@cli_catch_unknown_error
def sync(provider_name, provider_args):
    """
    Sync the Niamoto database with a data provider.
    """
    from niamoto.api.data_provider_api import sync_with_data_provider
    click.echo("Syncing the Niamoto database with '{}'...".format(
        provider_name)
    )
    try:
        r = sync_with_data_provider(provider_name, *provider_args)
        o = r['occurrence']
        o_i, o_u, o_d = \
            len(o['insert']), \
            len(o['update']), \
            len(o['delete'])
        p = r['plot']
        p_i, p_u, p_d = \
            len(p['insert']), \
            len(p['update']), \
            len(p['delete'])
        po = r['plot_occurrence']
        po_i, po_u, po_d = \
            len(po['insert']), \
            len(po['update']), \
            len(po['delete'])
        occ_change = o_i > 0 or o_u > 0 or o_d > 0
        plot_change = p_i > 0 or p_u > 0 or p_d > 0
        plot_occ_change = po_i > 0 or po_u > 0 or po_d > 0
        if occ_change or plot_change or plot_occ_change:
            m = "The Niamoto database had been successfully synced " \
                "with '{}'!\nBellow is a summary of what had been done:"
            click.echo(m.format(provider_name))
        else:
            m = "The Niamoto database was already up to date with '{}', " \
                "nothing had been done."
            click.echo(m.format(provider_name))
        if occ_change:
            click.secho("    Occurrences:")
            click.secho("        {} inserted".format(o_i), fg='green')
            click.secho("        {} updated".format(o_u), fg='yellow')
            click.secho("        {} deleted".format(o_d), fg='red')
        if plot_change:
            click.secho("    Plots:")
            click.secho("        {} inserted".format(p_i), fg='green')
            click.secho("        {} updated".format(p_u), fg='yellow')
            click.secho("        {} deleted".format(p_d), fg='red')
        if plot_occ_change:
            click.secho("    Plots / Occurrences:")
            click.secho("        {} inserted".format(po_i), fg='green')
            click.secho("        {} updated".format(po_u), fg='yellow')
            click.secho("        {} deleted".format(po_d), fg='red')
    except NoRecordFoundError as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)
    except BaseDataProviderException as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)
