# coding: utf-8

from niamoto.data_providers.base_data_provider import BaseDataProvider


class TestDataProvider(BaseDataProvider):

    @classmethod
    def get_type_name(cls):
        return "TEST_DATA_PROVIDER"
