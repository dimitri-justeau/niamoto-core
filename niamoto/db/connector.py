# coding: utf-8

from sqlalchemy import create_engine

from niamoto.settings import NIAMOTO_SCHEMA, DEFAULT_DATABASE


class Connector:
    """
    Class managing engines and connections to database(s).
    """

    ENGINES = {}

    @classmethod
    def get_connection(cls, database=DEFAULT_DATABASE, schema=NIAMOTO_SCHEMA):
        """
        :return: Return a sqlalchemy connection on a postgresql database.
        """
        engine = cls.get_engine(database=database)
        return engine.connect().execution_options(
            schema_translate_map={
                None: schema,
            }
        )

    @classmethod
    def dispose_engines(cls):
        for i in cls.ENGINES.values():
            i.dispose()

    @classmethod
    def get_engine(cls, database=DEFAULT_DATABASE):
        """
        :return: Return a sqlalchemy engine, use internal cache to avoid
        engine duplicates.
        """
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
