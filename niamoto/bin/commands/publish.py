# coding: utf-8

import sys

import pandas as pd
import click

from niamoto.decorators import cli_catch_unknown_error
from niamoto.exceptions import BaseDataPublisherException


@click.command(
    "publish",
    context_settings={
        'ignore_unknown_options': True,
        # 'allow_extra_args': True,
    }
)
@click.argument("publisher_key")
@click.argument("publish_format")
@click.option('--destination', '-d', default=sys.stdout)
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
@cli_catch_unknown_error
def publish_cli(publisher_key, publish_format, destination, *args, **kwargs):
    """
    Process and publish data.
    """
    from niamoto.api import publish_api
    try:
        extra_args = []
        extra_kwargs = {}
        pass_next = False
        for i, v in enumerate(kwargs['args']):
            if pass_next:
                pass_next = False
                continue
            if v[:2] == '--':
                extra_kwargs[v[2:]] = kwargs['args'][i + 1]
                pass_next = True
            else:
                extra_args.append(v)
        publish_api.publish(
            publisher_key,
            publish_format,
            *extra_args,
            destination=destination,
            **extra_kwargs
        )
    except BaseDataPublisherException as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)


@click.command("publishers")
@cli_catch_unknown_error
def list_publishers_cli():
    """
    Display the list of available data publishers.
    """
    from niamoto.data_publishers.base_data_publisher import PUBLISHER_REGISTRY
    publishers_keys = list(PUBLISHER_REGISTRY.keys())
    max_length = max([len(i) for i in publishers_keys])
    for k in publishers_keys:
        click.echo(
            "    {} :   {}".format(
                k.ljust(max_length),
                PUBLISHER_REGISTRY[k]['description']
            )
        )


@click.command("publish_formats")
@click.argument("publisher_key")
@cli_catch_unknown_error
def list_publish_formats_cli(publisher_key):
    """
    Display the list of available publish formats for a given publisher.
    """
    from niamoto.api.publish_api import list_publish_formats
    for f in list_publish_formats(publisher_key):
        click.secho('    {}'.format(f))

