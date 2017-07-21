# coding: utf-8

import sys
import inspect

import click

from niamoto.decorators import cli_catch_unknown_error


def make_publish_format_func(publish_key, publish_format):
    from niamoto.api import publish_api

    @click.option('--destination', '-d', default=sys.stdout)
    @click.pass_context
    @cli_catch_unknown_error
    def func(ctx, *args, destination=sys.stdout, **kwargs):
        kwargs.update(ctx.obj)
        publish_api.publish(
            publish_key,
            publish_format,
            *args,
            destination=destination,
            **kwargs
        )
    return func


@click.group("publish")
def publish_cli():
    pass


def init_publish_cli():
    from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
    from niamoto.data_publishers.base_data_publisher import PUBLISHER_REGISTRY
    from niamoto.utils import parse_docstring

    publish_commands = {}

    for pub_key in PUBLISHER_REGISTRY.keys():

        publisher = PUBLISHER_REGISTRY[pub_key]['class']

        try:
            publisher.get_publish_formats()
        except NotImplementedError:
            continue

        @click.pass_context
        @cli_catch_unknown_error
        def group(ctx, **kwargs):
            ctx.obj = kwargs

        signature = inspect.signature(publisher._process)
        doc = parse_docstring(publisher._process.__doc__)

        for p_key, p in signature.parameters.items():
            if p_key == 'self':
                continue
            if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                group = click.argument(p_key)(group)
            elif p.kind == inspect.Parameter.KEYWORD_ONLY:
                h = ""
                if p_key in doc['params']:
                    h = doc['params'][p_key].replace("\n", " ")
                default = p.default
                arg_type = str
                if default is not None:
                    arg_type = type(default)
                flag = False
                if arg_type == bool:
                    flag = True
                group = click.option(
                    "--" + p_key,
                    type=arg_type,
                    default=default,
                    help=h,
                    is_flag=flag,
                )(group)

        group = publish_cli.group(
            pub_key,
            context_settings={'ignore_unknown_options': True, },
            help=publisher.get_description(),
        )(group)

        publish_formats = publisher.get_publish_formats()
        for pub_format in publish_formats:
            format_method = BaseDataPublisher.FORMAT_TO_METHOD[pub_format]
            signature = inspect.signature(format_method)
            format_doc = parse_docstring(format_method.__doc__)
            f = make_publish_format_func(pub_key, pub_format)
            for p_key, p in signature.parameters.items():
                if p_key in ['data', 'destination']:
                    continue
                if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                    f = click.argument(p_key)(f)
                elif p.kind == inspect.Parameter.KEYWORD_ONLY:
                    h = ""
                    if p_key in format_doc['params']:
                        h = format_doc['params'][p_key].replace("\n", " ")
                    default = p.default
                    arg_type = str
                    if default is not None:
                        arg_type = type(default)
                    flag = False
                    if arg_type == bool:
                        flag = True
                    f = click.option(
                        "--" + p_key,
                        type=arg_type,
                        default=default,
                        help=h,
                        is_flag=flag,
                    )(f)

            group.command(
                pub_format,
                help=format_doc['short_description'],
            )(f)

        publish_commands[pub_key] = group


@click.command("publishers")
@cli_catch_unknown_error
def list_publishers_cli():
    """
    Display the list of available data publishers.
    """
    from niamoto.data_publishers.base_data_publisher import PUBLISHER_REGISTRY
    max_length = max([len(i) for i in PUBLISHER_REGISTRY.keys()])
    for k in PUBLISHER_REGISTRY.keys():
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
    from niamoto.api import publish_api
    from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
    keys = publish_api.list_publish_formats(publisher_key)
    max_length = max([len(i) for i in keys])
    for k in keys:
        click.secho(
            '    {} :    {}'.format(
                k.ljust(max_length),
                BaseDataPublisher.PUBLISH_FORMATS_DESCRIPTION[k]
            )
        )
