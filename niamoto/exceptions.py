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
