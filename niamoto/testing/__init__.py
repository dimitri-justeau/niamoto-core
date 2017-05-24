# coding: utf-8

import os

from niamoto import conf


PATH = os.path.abspath(os.path.dirname(__file__))
PROJECT_PATH = os.path.dirname(os.path.dirname(PATH))

TEST_PATH = os.path.join(
    PROJECT_PATH,
    "tests",
    "test_data",
    "test_niamoto_home"
)


def set_test_path():
    os.environ['NIAMOTO_HOME'] = TEST_PATH
    conf.set_niamoto_home()
    conf.set_settings()
