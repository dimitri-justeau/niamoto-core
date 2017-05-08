# coding: utf-8

import os


PATH = os.path.abspath(os.path.dirname(__file__))
PROJECT_PATH = os.path.dirname(os.path.dirname(PATH))


def set_test_path():
    os.environ['NIAMOTO_HOME'] = os.path.join(
        PROJECT_PATH,
        "tests",
        "test_data",
        "test_niamoto_home"
    )
