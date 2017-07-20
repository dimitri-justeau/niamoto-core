# coding: utf-8


class NiamotoException(Exception):
    """
    Base class for Niamoto exceptions.
    """
    pass


class ImproperlyConfiguredError(NiamotoException):
    """
    Error to raise when Niamoto is not properly configured.
    """
    pass


class IncoherentDatabaseStateError(NiamotoException):
    """
    Error to raise when a incoherent database state is detected.
    """
    pass


class NoRecordFoundError(NiamotoException):
    """
    Error to raise when a record had not been found in database.
    """
    pass


class RecordAlreadyExistsError(NiamotoException):
    """
    Error to raise when trying to insert a record that already exists.
    """


class BaseDataProviderException(NiamotoException):
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


class BaseDataPublisherException(NiamotoException):
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


class BaseDataMartException(NiamotoException):
    """
    Base class for errors related to data marts.
    """


class BaseDimensionException(BaseDataMartException):
    """
    Base class for errors related to data mart dimensions.
    """


class DimensionNotRegisteredError(BaseDimensionException):
    """
    Error to raise when trying to load a non registered dimension.
    """
