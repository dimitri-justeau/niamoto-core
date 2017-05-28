# coding: utf-8

from niamoto.conf import settings


def resolve_database(f):
    """
    When a function takes the 'database' keyword argument, resolve the default
    database is None is passed.
    """
    def func(*args, **kwargs):
        database = kwargs['database']
        if database is None:
            kwargs['database'] = settings.DEFAULT_DATABASE
        return f(*args, **kwargs)
    return func

