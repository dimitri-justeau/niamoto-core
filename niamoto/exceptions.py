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


class RecordAlreadyExists(Exception):
    """
    Error to raise when trying to insert a record that already exists.
    """
