# coding: utf-8

import importlib
import importlib.util

from niamoto.exceptions import ImproperlyConfiguredError


DEFAULT_SETTINGS = {
    'NIAMOTO_SCHEMA': 'niamoto',
    'NIAMOTO_RASTER_SCHEMA': 'niamoto_raster',
    'NIAMOTO_VECTOR_SCHEMA': 'niamoto_vector',
}


class DynamicSettings:
    """
    Niamoto dynamic settings endpoint.
    Inspired by Django's method to handle dynamic settings modules.
    Loads a set of settings value from a python settings file.
    """

    def __init__(self, settings_module_path=None):
        self._settings_module_path = settings_module_path
        self.update_settings_values()

    @property
    def settings_module_path(self):
        return self._settings_module_path

    @settings_module_path.setter
    def settings_module_path(self, value):
        self._settings_module_path = value
        self.update_settings_values()

    def update_settings_values(self):
        try:
            settings_module = None
            if self.settings_module_path is not None:
                spec = importlib.util.spec_from_file_location(
                    "settings",
                    self.settings_module_path
                )
                settings_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(settings_module)
        except ImportError:
            raise ImproperlyConfiguredError(
                    "There was an error importing the settings "
                    "module: '{}'".format(
                        self.settings_module_path
                    )
            )
        for setting in dir(settings_module):
            if setting.startswith('_') and setting != '__file__':
                continue
            setting_value = getattr(settings_module, setting)
            setattr(self, setting, setting_value)
