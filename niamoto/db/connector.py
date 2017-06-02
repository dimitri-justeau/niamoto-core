# coding: utf-8

from contextlib import contextmanager

from sqlalchemy import create_engine

from niamoto.conf import settings


class Connector:
    """
    Class managing engines and connections to database(s).
    """

    ENGINES = {}

    @classmethod
    @contextmanager
    def get_connection(cls):
        """
        :return: Return a sqlalchemy connection on a postgresql database.
        """
        try:
            engine = cls.get_engine()
            connection = engine.connect()
            yield connection
        finally:
            connection.close()

    @classmethod
    def dispose_engines(cls):
        for i in cls.ENGINES.values():
            i.dispose()

    @classmethod
    def get_engine(cls):
        """
        :return: Return a sqlalchemy engine, use internal cache to avoid
        engine duplicates.
        """
        database = settings.NIAMOTO_DATABASE
        user = database['USER']
        password = database['PASSWORD']
        host = database['HOST']
        database = database['NAME']
        db_url = 'postgresql+psycopg2://{user}:{password}@{host}/{database}'
        db_url = db_url.format(**{
            'user': user,
            'password': password,
            'host': host,
            'database': database,
        })
        if db_url not in cls.ENGINES:
            engine = create_engine(db_url)
            cls.ENGINES[db_url] = engine
        return cls.ENGINES[db_url]
