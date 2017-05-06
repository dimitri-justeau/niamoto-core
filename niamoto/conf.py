# coding: utf-8

import importlib
import os


SETTINGS_ENVIRONMENT_VARIABLE = "NIAMOTO_SETTINGS_MODULE"


class DynamicSettings:
    """
    Niamoto dynamic settings endpoint.
    Inspired by Django's method to handle dynamic settings modules.
    """

    def __init__(self, settings_module_path):
        self.setting_module_path = settings_module_path
        setting_module = importlib.import_module(settings_module_path)
        for setting in dir(setting_module):
            setting_value = getattr(setting_module, setting)
            setattr(self, setting, setting_value)


class ImproperlyConfiguredError(Exception):
    pass


settings = DynamicSettings(os.environ.get(SETTINGS_ENVIRONMENT_VARIABLE))
if not settings:
    raise ImproperlyConfiguredError("Niamoto is not properly configured.")
