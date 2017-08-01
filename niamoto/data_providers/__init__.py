# coding: utf-8

import os
import platform

from niamoto.data_providers.base_data_provider import BaseDataProvider
from niamoto.data_providers.base_occurrence_provider import \
    BaseOccurrenceProvider
from niamoto.data_providers.base_plot_provider import BasePlotProvider
from niamoto.data_providers.base_plot_occurrence_provider import \
    BasePlotOccurrenceProvider

from niamoto.data_providers.csv_provider import CsvDataProvider
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider

from niamoto.conf import NIAMOTO_HOME

PYTHON_PROVIDERS_HOME = os.path.join(NIAMOTO_HOME, 'python', 'providers')

if not os.path.exists(PYTHON_PROVIDERS_HOME):
    os.mkdir(PYTHON_PROVIDERS_HOME)

# Load Python data providers
py_version = platform.python_version_tuple()
for file in os.listdir(PYTHON_PROVIDERS_HOME):
    if file.endswith(".py"):
        if int(py_version[1]) >= 5:
            import importlib
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                file[:-3],
                os.path.join(PYTHON_PROVIDERS_HOME, file)
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            from importlib.machinery import SourceFileLoader
            settings_module = SourceFileLoader(
                file[:-3],
                os.path.join(PYTHON_PROVIDERS_HOME, file)
            ).load_module()
