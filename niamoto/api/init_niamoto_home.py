# coding: utf-8

import os
from shutil import copyfile

from niamoto.conf import set_niamoto_home, NIAMOTO_SETTINGS
from niamoto.constants import DEFAULT_NIAMOTO_HOME


def init_niamoto_home(niamoto_home_path=DEFAULT_NIAMOTO_HOME):
    """
    Initialize the niamoto home directory.
    :param niamoto_home_path: The path where to initialize the niamoto home
    directory.
    """
    if os.path.exists(niamoto_home_path) and os.path.isdir(niamoto_home_path):
        raise FileExistsError("The directory {} already exists.".format(
            niamoto_home_path
        ))
    os.mkdir(niamoto_home_path)
    set_niamoto_home(niamoto_home_path)
    from niamoto.settings import default_settings
    path = os.path.abspath(default_settings.__file__)
    copyfile(path, os.path.join(niamoto_home_path, NIAMOTO_SETTINGS + '.py'))
