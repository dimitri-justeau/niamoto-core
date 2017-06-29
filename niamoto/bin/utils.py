# coding: utf-8

import pandas as pd
from click.core import Command


def format_datetime_to_date(obj):
    if pd.isnull(obj):
        return ''
    return obj.strftime("%Y/%m/%d")


# class CustomHelpCommand(Command):
#     """
#     A custom command class where it is possible to change the --help output.
#     """
#
#     def __init__(self, name, context_settings=None, callback=None,
#                  params=None, help=None, epilog=None, short_help=None,
#                  options_metavar='[OPTIONS]', add_help_option=True,
#                  hidden=False):
#         super(CustomHelpCommand, self).__init__(
#             self, name, context_settings=context_settings, callback=callback,
#             params=params, help=help, epilog=epilog, short_help=short_help,
#             options_metavar='[OPTIONS]', add_help_option=True,
#             hidden=False
#         )
