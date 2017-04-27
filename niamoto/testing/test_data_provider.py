# coding: utf-8

from niamoto.data_providers.base_data_provider import BaseDataProvider


class TestDataProvider(BaseDataProvider):

    @property
    def plot_provider(self):
        raise NotImplementedError()

    @property
    def occurrence_provider(self):
        raise NotImplementedError()

    @classmethod
    def get_type_name(cls):
        return "TEST_DATA_PROVIDER"
