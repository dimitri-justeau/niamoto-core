# coding: utf-8

import traceback

import click

from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def cli_catch_unknown_error(f):
    def func(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except SystemExit:
            click.get_current_context().exit(code=1)
        except:
            click.secho(
                "An unknown error occurred, please see the logs for "
                "more details.",
                fg='red'
            )
            LOGGER.debug(traceback.format_exc())
            click.get_current_context().exit(code=1)
    func.__doc__ = f.__doc__
    return func
