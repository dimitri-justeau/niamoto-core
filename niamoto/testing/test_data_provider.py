# coding: utf-8

from niamoto.data_providers import *


class TestDataProvider(BaseDataProvider):

    def __init__(self, name):
        super(TestDataProvider, self).__init__(name)
        self._occurrence_provider = BaseOccurrenceProvider(self)
        self._plot_provider = BasePlotProvider(self)
        self._plot_occurrence_provider = BasePlotOccurrenceProvider(self)

    @property
    def occurrence_provider(self):
        return self._occurrence_provider

    @property
    def plot_occurrence_provider(self):
        return self._plot_occurrence_provider

    @property
    def plot_provider(self):
        return self._plot_provider

    @classmethod
    def get_type_name(cls):
        return "TEST_DATA_PROVIDER"
