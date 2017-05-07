# coding: utf-8

import os


PATH = os.path.abspath(os.path.dirname(__file__))


def set_test_path():
    os.environ['NIAMOTO_HOME'] = os.path.join(
        PATH, "test_niamoto_home"
    )
