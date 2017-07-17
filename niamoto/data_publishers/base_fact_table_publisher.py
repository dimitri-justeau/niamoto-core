# coding: utf-8

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher


class BaseFactTablePublisher(BaseDataPublisher):
    """
    Base class for publishers populating fact tables.
    """

    def __init__(self, fact_table):
        self.fact_table = fact_table
        super(BaseFactTablePublisher, self).__init__()

    def _process(self, *args, **kwargs):
        raise NotImplementedError()

    @classmethod
    def get_description(cls):
        pass

    @classmethod
    def get_key(cls):
        raise NotImplementedError()

    @classmethod
    def get_publish_formats(cls):
        raise NotImplementedError()
