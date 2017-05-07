# coding: utf-8

import importlib

from niamoto.exceptions import ImproperlyConfiguredError


class DynamicSettings:
    """
    Niamoto dynamic settings endpoint.
    Inspired by Django's method to handle dynamic settings modules.
    Loads a set of settings value from a python settings file.
    """

    def __init__(self, settings_module_path):
        self.setting_module_path = settings_module_path
        try:
            setting_module = importlib.import_module(settings_module_path)
        except ModuleNotFoundError:
            raise ImproperlyConfiguredError(
                    "The settings module '{}' does not exist.".format(
                        settings_module_path
                    )
            )
        for setting in dir(setting_module):
            setting_value = getattr(setting_module, setting)
            setattr(self, setting, setting_value)
