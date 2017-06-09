# coding: utf-8

from niamoto.data_publishers.base_data_publisher import PUBLISHER_REGISTRY


def publish(publisher_key, publish_format, destination, *args, **kwargs):
    """
    Api method for processing and publishing data.
    :param publisher_key:
    :param publish_format:
    :param destination
    :param args:
    :param kwargs:
    :return:
    """
    # TODO: handle wrong key
    # TODO: handle wrong format
    publisher = PUBLISHER_REGISTRY[publisher_key]
    publisher_instance = publisher['class']()
    data = publisher_instance.process(*args, **kwargs)
    publisher_instance.publish(data, publish_format, destination)
