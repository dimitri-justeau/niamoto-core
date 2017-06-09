# coding: utf-8


PUBLISHER_REGISTRY = {}


class PublisherMeta(type):

    def __init__(cls, *args, **kwargs):
        try:
            PUBLISHER_REGISTRY[cls.get_key()] = {
                'class': cls,
            }
        except NotImplementedError:
            pass
        return super(PublisherMeta, cls).__init__(cls)


class BaseDataPublisher(metaclass=PublisherMeta):
    """
    Base class for data publisher.
    """

    CSV = 'csv'
    PUBLISH_FORMATS = [CSV, ]

    def __init__(self):
        self.last_data = None
        self.last_args = None
        self.last_kwargs = None

    @classmethod
    def get_key(cls):
        raise NotImplementedError()

    def process(self, *args, **kwargs):
        """
        Process the data, memoize and return the result to be published.
        :return: The data to be published after processing.
        """
        self.last_data = self._process(*args, **kwargs)
        self.last_args = args
        self.last_kwargs = kwargs
        return self.last_data

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
    def publish(cls, data, publish_format, destination):
        """
        Publish the processed data.
        :param data: The data to publish.
        :param publish_format: The publishing format.
        :param destination: The destination (e.g. path to destination file,
            destination database file)
        """
        format_to_method = {
            cls.CSV: cls._publish_csv
        }
        return format_to_method[publish_format](data, destination)

    @staticmethod
    def _publish_csv(data, destination_path):
        """
        Publish the data in a csv file.
        :param data: The data to publish, assume that it is a pandas DataFrame.
        :param destination_path: The destination file path.
        """
        data.to_csv(destination_path)
