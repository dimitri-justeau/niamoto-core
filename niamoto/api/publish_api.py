# coding: utf-8

from niamoto.data_publishers.base_data_publisher import PUBLISHER_REGISTRY
from niamoto.exceptions import WrongPublisherKeyError, \
    UnavailablePublishFormat


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
    if publisher_key not in PUBLISHER_REGISTRY:
        m = "The publisher key '{}' does not exist.".format(publisher_key)
        raise WrongPublisherKeyError(m)
    publisher = PUBLISHER_REGISTRY[publisher_key]
    publisher_instance = publisher['class']()
    if publish_format not in publisher_instance.get_publish_formats():
        m = "The publish format '{}' is unavailable with the '{}' publisher."
        raise UnavailablePublishFormat(
            m.format(publish_format, publisher_key)
        )
    data, p_args, p_kwargs = publisher_instance.process(*args, **kwargs)
    publisher_instance.publish(
        data,
        publish_format,
        destination,
        *p_args,
        **p_kwargs
    )
