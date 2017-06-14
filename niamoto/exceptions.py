# coding: utf-8


class ImproperlyConfiguredError(Exception):
    """
    Error to raise when Niamoto is not properly configured.
    """
    pass


class IncoherentDatabaseStateError(Exception):
    """
    Error to raise when a incoherent database state is detected.
    """
    pass


class NoRecordFoundError(Exception):
    """
    Error to raise when a record had not been found in database.
    """
    pass


class RecordAlreadyExistsError(Exception):
    """
    Error to raise when trying to insert a record that already exists.
    """


class BaseDataProviderException(Exception):
    """
    Base class for errors specific to data providers implementations.
    """


class DataSourceNotFoundError(BaseDataProviderException):
    """
    Error to raise when a data source cannot be found (e.g. file, database).
    """


class MalformedDataSourceError(BaseDataProviderException):
    """
    Error to raise when a data source is malformed.
    """


class BaseDataPublisherException(Exception):
    """
    Base class for errors specific to data publisher.
    """


class WrongPublisherKeyError(BaseDataPublisherException):
    """
    Error to raise when invoking a publisher with a wrong key/
    """


class UnavailablePublishFormat(BaseDataPublisherException):
    """
    Error to raise when invoking an unavailable publish format.
    """
