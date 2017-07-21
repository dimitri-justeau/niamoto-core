# coding: utf-8

import traceback

import click

from niamoto.log import get_logger
from niamoto.exceptions import NiamotoException


LOGGER = get_logger(__name__)


def cli_catch_unknown_error(f):
    def func(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except (NiamotoException, FileNotFoundError) as e:
            click.secho(str(e), fg='red')
            click.get_current_context().exit(code=1)
        except SystemExit:
            click.get_current_context().exit(code=1)
        except Exception as e:
            click.secho(str(e), fg='red')
            click.secho(
                "Please see the logs for more details.",
                fg='red'
            )
            LOGGER.debug(traceback.format_exc())
            click.get_current_context().exit(code=1)
    func.__doc__ = f.__doc__
    return func
