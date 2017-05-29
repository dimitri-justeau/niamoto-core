# coding: utf-8

from sqlalchemy import select, Index

from niamoto.conf import settings
from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.exceptions import NoRecordFoundError, RecordAlreadyExists


class BaseDataProvider:
    """
    Abstract base class for plot and occurrence data providers.
    """

    def __init__(self, name, database=settings.DEFAULT_DATABASE):
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

    @property
    def plot_occurrence_provider(self):
        raise NotImplementedError()

    def sync(self, insert=True, update=True, delete=True,
             sync_occurrence=True, sync_plot=True,
             sync_plot_occurrence=True):
        """
        Sync Niamoto database with providers data.
        :param insert: if False, skip insert operation.
        :param update: if False, skip update operation.
        :param delete: if False, skip delete operation.
        :param sync_occurrence: if False, skip occurrence sync.
        :param sync_plot: if False, skip plot sync.
        :param sync_plot_occurrence: if skip plot-occurrence sync.
        :return A dict containing the insert / update / delete dataframes for
        each specialized provider:
            {
                'occurrence': {
                    'insert': insert_df,
                    'update': update_df,
                    "delete': delete_df,
                },
                'plot': { ... },
                'plot_occurrence': { ... },
            }
        """
        with Connector.get_connection(database=self.database) as connection:
            with connection.begin():
                i1, u1, d1 = self.occurrence_provider.sync(
                    connection,
                    insert=insert,
                    update=update,
                    delete=delete,
                ) if sync_occurrence else ([], [], [])
                i2, u2, d2 = self.plot_provider.sync(
                    connection,
                    insert=insert,
                    update=update,
                    delete=delete,
                ) if sync_plot else ([], [], [])
            with connection.begin():
                i3, u3, d3 = self.plot_occurrence_provider.sync(
                    connection,
                    insert=insert,
                    update=update,
                    delete=delete,
                ) if sync_plot_occurrence else ([], [], [])
            return {
                'occurrence': {
                    'insert': i1,
                    'update': u1,
                    'delete': d1,
                },
                'plot': {
                    'insert': i2,
                    'update': u2,
                    'delete': d2,
                },
                'plot_occurrence': {
                    'insert': i3,
                    'update': u3,
                    'delete': d3,
                },
            }

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
    def _unregister_unique_synonym_constraint(cls, connection):
        index = Index(
            "{}_unique_synonym".format(cls.get_type_name()),
            niamoto_db_meta.taxon.c.synonyms[cls.get_type_name()],
            unique=True,
        )
        niamoto_db_meta.taxon.indexes.remove(index)
        index.drop(connection)

    @classmethod
    def register_data_provider(cls, name, *args,
                               database=settings.DEFAULT_DATABASE,
                               properties={}, return_object=True, **kwargs):
        cls.assert_data_provider_does_not_exist(name, database)
        ins = niamoto_db_meta.data_provider.insert({
            'name': name,
            'provider_type_id': cls.get_data_provider_type_db_id(
                database=database
            ),
            'properties': properties,
        })
        with Connector.get_connection(database=database) as connection:
            connection.execute(ins)
        if return_object:
            return cls(name, *args, database=database, **kwargs)

    @classmethod
    def unregister_data_provider(cls, name,
                                 database=settings.DEFAULT_DATABASE):
        """
        Unregister a data provider from the database.
        :param name: The name of the data provider to unregister.
        :param database: The database to work with.
        """
        cls.assert_data_provider_exists(name, database)
        delete_stmt = niamoto_db_meta.data_provider.delete().where(
            niamoto_db_meta.data_provider.c.name == name
        )
        with Connector.get_connection(database=database) as connection:
            with connection.begin():
                connection.execute(delete_stmt)

    @staticmethod
    def assert_data_provider_does_not_exist(name, database):
        sel = niamoto_db_meta.data_provider.select().where(
            niamoto_db_meta.data_provider.c.name == name
        )
        with Connector.get_connection(database=database) as connection:
            r = connection.execute(sel).rowcount
            if r > 0:
                m = "The data provider '{}' already exists in database."
                raise RecordAlreadyExists(m.format(name))

    @staticmethod
    def assert_data_provider_exists(name, database):
        sel = niamoto_db_meta.data_provider.select().where(
            niamoto_db_meta.data_provider.c.name == name
        )
        with Connector.get_connection(database=database) as connection:
            r = connection.execute(sel).rowcount
            if r == 0:
                m = "The data provider '{}' does not exist in database."
                raise NoRecordFoundError(m.format(name))
