# coding: utf-8

import importlib
import os
import sys


#  Environment variable indicating the Niamoto home folder.
HOME_ENVIRONMENT_VARIABLE = "NIAMOTO_HOME"

DEFAULT_NIAMOTO_HOME = "~/niamoto"
NIAMOTO_SETTINGS = "settings"


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


class ImproperlyConfiguredError(Exception):
    pass


#  Set home var if not set
if HOME_ENVIRONMENT_VARIABLE not in os.environ:
    NIAMOTO_HOME = DEFAULT_NIAMOTO_HOME
else:
    NIAMOTO_HOME = os.environ[HOME_ENVIRONMENT_VARIABLE]

# Create home folder if not existing
if not os.path.exists(NIAMOTO_HOME):
    os.makedirs(NIAMOTO_HOME)

# Append NIAMOTO_HOME to sys.path
sys.path.append(NIAMOTO_HOME)

#  Set global settings
settings = DynamicSettings(NIAMOTO_SETTINGS)
if not settings:
    raise ImproperlyConfiguredError("Niamoto is not properly configured.")


def set_settings(settings_module_path):
    global settings
    settings = DynamicSettings(settings_module_path)
