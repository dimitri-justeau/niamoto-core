# coding: utf-8

import os
import sys

from niamoto.exceptions import ImproperlyConfiguredError
from niamoto.settings.dynamic_settings import DynamicSettings


#  Environment variable indicating the Niamoto home folder.
HOME_ENVIRONMENT_VARIABLE = "NIAMOTO_HOME"

DEFAULT_NIAMOTO_HOME = "~/niamoto"
NIAMOTO_SETTINGS = "settings"


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
