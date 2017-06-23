# coding: utf-8

import sys


PUBLISHER_REGISTRY = {}


class PublisherMeta(type):

    def __init__(cls, *args, **kwargs):
        try:
            PUBLISHER_REGISTRY[cls.get_key()] = {
                'class': cls,
                'description': cls.get_description()
            }
        except NotImplementedError:
            pass
        return super(PublisherMeta, cls).__init__(cls)


class BaseDataPublisher(metaclass=PublisherMeta):
    """
    Base class for data publisher.
    """

    STREAM = 'stream'
    CSV = 'csv'
    TIFF = 'tiff'
    PUBLISH_FORMATS = [CSV, STREAM, TIFF]
    PUBLISH_FORMATS_DESCRIPTION = {
        CSV: "Publish the data using the csv format.",
        STREAM: "Publish the data as a text stream.",
        TIFF: "Publish the data as a tiff raster file.",
    }

    def __init__(self):
        self.last_data = None
        self.last_publish_args = None
        self.last_publish_kwargs = None
        self.last_args = None
        self.last_kwargs = None

    @classmethod
    def get_key(cls):
        raise NotImplementedError()

    @classmethod
    def get_description(cls):
        raise NotImplementedError()

    def process(self, *args, **kwargs):
        """
        Process the data, memoize and return the result to be published.
        :return: The data to be published after processing, the publish args
            and the publish kwargs.
        """
        r = self._process(*args, **kwargs)
        if not isinstance(r, (list, tuple)):
            r = [r, [], {}]
        self.last_data = r[0]
        self.last_publish_args = r[1]
        self.last_publish_kwargs = r[2]
        self.last_args = args
        self.last_kwargs = kwargs
        return r[0], r[1], r[2]

    def _process(self, *args, **kwargs):
        """
        Process the data and return the result to be published.
        :return: The data to be published after processing.
        """
        raise NotImplementedError()

    @classmethod
    def get_publish_formats(cls):
        """
        :return: A list of keys corresponding to the supported publish formats
        by the data publisher.
        """
        raise NotImplementedError()

    @classmethod
    def publish(cls, data, publish_format, *args, destination=sys.stdout,
                **kwargs):
        """
        Publish the processed data.
        :param data: The data to publish.
        :param publish_format: The publishing format.
        :param destination: The destination (e.g. path to destination file,
            destination database file)
        """
        return cls.FORMAT_TO_METHOD[publish_format](
            data,
            *args,
            destination=destination,
            **kwargs
        )

    @staticmethod
    def _publish_csv(data, *args, destination=sys.stdout, index_label=None,
                     **kwargs):
        """
        Publish the data in a csv file.
        :param data: The data to publish, assume that it is a pandas DataFrame.
        :param destination_path: The destination file path.
        """
        data.to_csv(destination, index_label=index_label)

    @staticmethod
    def _publish_stream(data, *args, destination=sys.stdout, **kwargs):
        """
        Publish the data in a stream.
        :param data: The data to publish, assume that it is a pandas DataFrame.
        :param destination_path: The destination stream.
        """
        data.to_string(buf=destination)
        destination.write('\n')

    FORMAT_TO_METHOD = {
        CSV: _publish_csv.__func__,
        STREAM: _publish_stream.__func__,
    }
