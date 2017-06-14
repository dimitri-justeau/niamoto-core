# coding: utf-8

import click

from niamoto.decorators import cli_catch_unknown_error
from niamoto.exceptions import BaseDataPublisherException


@click.command(
    "publish",
    context_settings={
        'ignore_unknown_options': True,
        'allow_extra_args': True,
    }
)
@click.argument("publisher_key")
@click.argument("publish_format")
@click.argument("destination")
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
@cli_catch_unknown_error
def publish_cli(publisher_key, publish_format, destination, *args, **kwargs):
    """
    Process and publish data.
    """
    from niamoto.api import publish_api
    try:
        publish_api.publish(
            publisher_key,
            publish_format,
            destination,
            *args,
            **kwargs
        )
    except BaseDataPublisherException as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


