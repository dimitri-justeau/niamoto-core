# coding: utf-8

import os
import platform

from niamoto.conf import NIAMOTO_HOME
from niamoto.data_publishers.occurrence_data_publisher import \
    OccurrenceDataPublisher
from niamoto.data_publishers.plot_data_publisher import PlotDataPublisher
from niamoto.data_publishers.taxon_data_publisher import TaxonDataPublisher
from niamoto.data_publishers.plot_occurrence_data_publisher import \
    PlotOccurrenceDataPublisher
from niamoto.data_publishers.r_data_publisher import RDataPublisher
from niamoto.data_publishers.raster_data_publisher import RasterDataPublisher

R_SCRIPTS_HOME = os.path.join(NIAMOTO_HOME, 'R')
PYTHON_SCRIPTS_HOME = os.path.join(NIAMOTO_HOME, 'python', 'publishers')

if not os.path.exists(R_SCRIPTS_HOME):
    os.mkdir(R_SCRIPTS_HOME)

if not os.path.exists(PYTHON_SCRIPTS_HOME):
    os.makedirs(PYTHON_SCRIPTS_HOME)


def create_r_publisher(file_path):
    key = "R_" + file_path[:-2]

    class RPublisher(RDataPublisher):

        def __init__(self):
            super(RPublisher, self).__init__(
                os.path.join(R_SCRIPTS_HOME, file_path)
            )

        @classmethod
        def get_key(cls):
            return key


# Load R data publishers
for file in os.listdir(R_SCRIPTS_HOME):
    if file.endswith(".R"):
        create_r_publisher(file)


# Load Python data publishers
py_version = platform.python_version_tuple()
for file in os.listdir(PYTHON_SCRIPTS_HOME):
    if file.endswith(".py"):
        if int(py_version[1]) >= 5:
            import importlib
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                file[:-3],
                os.path.join(PYTHON_SCRIPTS_HOME, file)
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            from importlib.machinery import SourceFileLoader
            settings_module = SourceFileLoader(
                file[:-3],
                os.path.join(PYTHON_SCRIPTS_HOME, file)
            ).load_module()
