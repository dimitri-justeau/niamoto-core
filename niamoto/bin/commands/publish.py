# coding: utf-8

import sys
import inspect

import click

from niamoto.decorators import cli_catch_unknown_error
from niamoto.exceptions import BaseDataPublisherException
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.data_publishers.base_data_publisher import PUBLISHER_REGISTRY
from niamoto.utils import parse_docstring
from niamoto.api import publish_api


PUBLISHERS_KEYS = list(PUBLISHER_REGISTRY.keys())
PUBLISH_COMMANDS = {}


@click.group("publish")
def publish_cli():
    pass


for pub_key in PUBLISHERS_KEYS:

    publisher = PUBLISHER_REGISTRY[pub_key]['class']

    @click.argument('publish_format')
    @click.option('--destination', '-d', default=sys.stdout)
    @click.argument('args', nargs=-1, type=click.UNPROCESSED)
    @cli_catch_unknown_error
    def f(publish_format, *args, destination=sys.stdout, **kwargs):
        try:
            extra_args = []
            extra_kwargs = {}
            first_key = False
            previous_is_key = False
            last_key = None
            for i, v in enumerate(kwargs['args']):
                if v[:2] == '--':
                    if i + 1 == len(kwargs['args']):
                        extra_kwargs[v[2:]] = True
                    if not first_key:
                        first_key = True
                        last_key = v[2:]
                    else:
                        if previous_is_key:
                            extra_kwargs[last_key] = True
                        last_key = v[2:]
                    previous_is_key = True
                else:
                    if not first_key:
                        extra_args.append(v)
                    else:
                        if previous_is_key:
                            extra_kwargs[last_key] = v
                        else:
                            value = extra_kwargs[last_key]
                            if isinstance(value, list):
                                value.append(v)
                            else:
                                value = [value, v]
                            extra_kwargs[last_key] = value
                        previous_is_key = False
            publish_api.publish(
                pub_key,
                publish_format,
                *extra_args,
                destination=destination,
                **extra_kwargs
            )
        except BaseDataPublisherException as e:
            click.secho(str(e), fg='red')
            click.get_current_context().exit(code=1)

    signature = inspect.signature(publisher._process)
    doc = parse_docstring(publisher._process.__doc__)

    for p_key, p in signature.parameters.items():
        if p_key == 'self':
            continue
        if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            f = click.argument(p_key)(f)
        elif p.kind == inspect.Parameter.KEYWORD_ONLY:
            h = ""
            if p_key in doc['params']:
                h = doc['params'][p_key].replace("\n", " ")
            default = p.default
            arg_type = str
            if default is not None:
                arg_type = type(default)
            f = click.option(
                "--" + p_key,
                type=arg_type,
                default=default,
                help=h,
            )(f)

    f = publish_cli.command(
        pub_key,
        context_settings={'ignore_unknown_options': True},
        help=publisher.get_description(),
    )(f)

    PUBLISH_COMMANDS[pub_key] = f


@click.command("publishers")
@cli_catch_unknown_error
def list_publishers_cli():
    """
    Display the list of available data publishers.
    """
    max_length = max([len(i) for i in PUBLISHERS_KEYS])
    for k in PUBLISHERS_KEYS:
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
    try:
        keys = publish_api.list_publish_formats(publisher_key)
        max_length = max([len(i) for i in keys])
        for k in keys:
            click.secho(
                '    {} :    {}'.format(
                    k.ljust(max_length),
                    BaseDataPublisher.PUBLISH_FORMATS_DESCRIPTION[k]
                )
            )
    except BaseDataPublisherException as e:
        click.secho(str(e), fg='red')
        click.get_current_context().exit(code=1)

