# coding: utf-8

import platform

if platform.python_version_tuple()[0] != '3':
    raise SystemError("Niamoto is only compatible with Python 3.")

from niamoto import conf
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


try:
    conf.set_niamoto_home()
    conf.set_settings()
except:
    LOGGER.warning("It seems like your $NIAMOTO_HOME does not contains "
                   "a settings file (settings.py)")


__version__ = "0.1"
