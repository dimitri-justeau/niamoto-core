# coding: utf-8

from sqlalchemy import select, Index

from niamoto.conf import settings
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector


class BaseDataProvider:
    """
    Abstract base class for plot and occurrence data providers.
    """

    def __init__(self, name, database=settings.DEFAULT_DATABASE):
        """
        :param db_id: The database id of the corresponding data
        provider record.
        """
        self.name = name
        self._db_id = None
        self._database = database
        self._update_db_id()

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, value):
        self._database = value
        self._update_db_id()

    @property
    def db_id(self):
        return self._db_id

    def _update_db_id(self):
        with Connector.get_connection(database=self.database) as connection:
            sel = select([niamoto_db_meta.data_provider.c.id]).where(
                niamoto_db_meta.data_provider.c.name == self.name
            )
            result = connection.execute(sel)
            self._db_id = result.fetchone()['id']

    @property
    def plot_provider(self):
        raise NotImplementedError()

    @property
    def occurrence_provider(self):
        raise NotImplementedError()

    @classmethod
    def get_type_name(cls):
        raise NotImplementedError()

    @classmethod
    def get_data_provider_type_db_id(cls, database=settings.DEFAULT_DATABASE):
        sel = select([niamoto_db_meta.data_provider_type.c.id]).where(
            niamoto_db_meta.data_provider_type.c.name == cls.get_type_name()
        )
        with Connector.get_connection(database=database) as connection:
            result = connection.execute(sel)
            return result.fetchone()['id']

    @classmethod
    def register_data_provider_type(cls, database=settings.DEFAULT_DATABASE):
        ins = niamoto_db_meta.data_provider_type.insert({
            'name': cls.get_type_name()
        })
        with Connector.get_connection(database=database) as connection:
            connection.execute(ins)
            cls._register_unique_synonym_constraint(database=database)

    @classmethod
    def _register_unique_synonym_constraint(
            cls,
            database=settings.DEFAULT_DATABASE):
        index = Index(
            "{}_unique_synonym".format(cls.get_type_name()),
            niamoto_db_meta.taxon.c.synonyms[cls.get_type_name()],
            unique=True,
        )
        engine = Connector.get_engine(database=database)
        index.create(engine)
        niamoto_db_meta.taxon.indexes.remove(index)

    @classmethod
    def register_data_provider(cls, name, database=settings.DEFAULT_DATABASE,
                               properties={}):
        ins = niamoto_db_meta.data_provider.insert({
            'name': name,
            'provider_type_id': cls.get_data_provider_type_db_id(
                database=database
            ),
            'properties': properties,
        })
        with Connector.get_connection(database=database) as connection:
            connection.execute(ins)
        return cls(name, database=database)
