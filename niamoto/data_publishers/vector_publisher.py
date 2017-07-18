# coding: utf-8

from niamoto.vector.vector_manager import VectorManager
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


class VectorDataPublisher(BaseDataPublisher):
    """
    Publish vectors from the niamoto vector database.
    """

    @classmethod
    def get_key(cls):
        return 'vector'

    @classmethod
    def get_description(cls):
        return "Publish a vector from the niamoto vector database."

    @classmethod
    def get_publish_formats(cls):
        return []

    def _process(self, vector_name, *args, **kwargs):
        """
        :param vector_name: The vector name in Niamoto vector database.
        :return: A GeoDataFrame corresponding to the vector to publish.
        """
        return VectorManager.get_vector_geo_dataframe(vector_name)
