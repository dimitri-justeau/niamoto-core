# coding: utf-8

from sqlalchemy import create_engine


class Connector:
    """
    TODO
    """

    ENGINES = {}

    @classmethod
    def get_connection(cls, user, password, host='localhost',
                       database='niamoto', schema='niamoto'):
        """
        :return: Return a sqlalchemy connection on a postgresql database.
        """
        engine = cls._get_engine(user, password, host=host, database=database)
        return engine.connect().execution_option(
            schema_translate_map={
                None: schema,
            }
        )

    @classmethod
    def _get_engine(cls, user, password, host='localhost', database='niamoto'):
        """
        :return: Return a sqlalchemy engine, use internal cache to avoid
        engine duplicates.
        """
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
