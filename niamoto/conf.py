# coding: utf-8

import os
import sys

from niamoto.constants import DEFAULT_NIAMOTO_HOME, HOME_ENVIRONMENT_VARIABLE
from niamoto.dynamic_settings import DynamicSettings
from niamoto.exceptions import ImproperlyConfiguredError

#  Environment variable indicating the Niamoto home folder.

NIAMOTO_SETTINGS = "settings.py"


#  Set home var if not set
if HOME_ENVIRONMENT_VARIABLE not in os.environ:
    NIAMOTO_HOME = DEFAULT_NIAMOTO_HOME
else:
    NIAMOTO_HOME = os.environ[HOME_ENVIRONMENT_VARIABLE]


def set_niamoto_home(niamoto_home_path=None):
    global NIAMOTO_HOME
    if NIAMOTO_HOME is sys.path:
        sys.path.remove(NIAMOTO_HOME)
    if niamoto_home_path is None:
        if HOME_ENVIRONMENT_VARIABLE not in os.environ:
            NIAMOTO_HOME = DEFAULT_NIAMOTO_HOME
        else:
            NIAMOTO_HOME = os.environ[HOME_ENVIRONMENT_VARIABLE]
    else:
        NIAMOTO_HOME = niamoto_home_path
        os.environ[HOME_ENVIRONMENT_VARIABLE] = NIAMOTO_HOME
    # Append NIAMOTO_HOME to sys.path
    sys.path.append(NIAMOTO_HOME)


# Create home folder if not existing
if not os.path.exists(NIAMOTO_HOME):
    os.makedirs(NIAMOTO_HOME)

# Global settings
settings = DynamicSettings()


def set_settings(settings_module_path=None):
    if settings_module_path is None:
        settings_module_path = os.path.join(
            NIAMOTO_HOME,
            NIAMOTO_SETTINGS
        )
    global settings
    settings.settings_module_path = settings_module_path
    if not settings:
        raise ImproperlyConfiguredError("Niamoto is not properly configured.")
