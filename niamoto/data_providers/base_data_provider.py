# coding: utf-8

import time
from datetime import datetime

from sqlalchemy import select, and_

from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.exceptions import NoRecordFoundError, RecordAlreadyExistsError
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


PROVIDER_REGISTRY = {}


class ProviderMeta(type):

    def __init__(cls, *args, **kwargs):
        try:
            PROVIDER_REGISTRY[cls.get_type_name()] = {
                'class': cls,
            }
        except NotImplementedError:
            pass
        return super(ProviderMeta, cls).__init__(cls)


class BaseDataProvider(metaclass=ProviderMeta):
    """
    Abstract base class for plot and occurrence data providers.
    """

    def __init__(self, name):
        self.name = name
        self._db_id = None
        self._synonym_key = None
        self._update_db_id()
        self._update_synonym_key()

    @property
    def db_id(self):
        return self._db_id

    @property
    def synonym_key(self):
        return self._synonym_key

    def _update_db_id(self):
        with Connector.get_connection() as connection:
            sel = select([niamoto_db_meta.data_provider.c.id]).where(
                niamoto_db_meta.data_provider.c.name == self.name
            )
            result = connection.execute(sel)
            self._db_id = result.fetchone()['id']

    def _update_synonym_key(self):
        with Connector.get_connection() as connection:
            synonym_fk_col = niamoto_db_meta.data_provider.c.synonym_key_id
            synonym_id_col = niamoto_db_meta.synonym_key_registry.c.id
            sel = select([niamoto_db_meta.synonym_key_registry.c.name]).where(
                and_(
                    synonym_id_col == synonym_fk_col,
                    niamoto_db_meta.data_provider.c.name == self.name
                )
            )
            result = connection.execute(sel)
            if result.rowcount > 0:
                self._synonym_key = result.fetchone()['name']

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
        t = time.time()
        LOGGER.debug("\r" + "-" * 80)
        LOGGER.info("*** Data sync starting ('{}' - {})...".format(
            self.name, self.get_type_name()
        ))
        with Connector.get_connection() as connection:
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
            upd = niamoto_db_meta.data_provider.update().values({
                'last_sync': datetime.now(),
            }).where(niamoto_db_meta.data_provider.c.name == self.name)
            connection.execute(upd)
            m = "*** Data sync with '{}' done (total time: {:.2f} s)!"
            LOGGER.info(m.format(
                self.name, time.time() - t
            ))
            LOGGER.debug("\r" + "-" * 80)
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
    def register_data_provider(cls, name, *args, properties={},
                               synonym_key=None, return_object=True, **kwargs):
        m = "DataProvider(name='{}', type_name='{}', properties='{}', " \
            "synonym_key='{}'): Registering data provider...'"
        LOGGER.debug(
            m.format(name, cls.get_type_name(), properties, synonym_key)
        )
        with Connector.get_connection() as connection:
            cls.assert_data_provider_does_not_exist(name, bind=connection)
            synonym_key_id = None
            if synonym_key is not None:
                synonym_key_id = TaxonomyManager.get_synonym_key(
                    synonym_key,
                    bind=connection
                )['id']
            ins = niamoto_db_meta.data_provider.insert({
                'name': name,
                'provider_type_key': cls.get_type_name(),
                'properties': properties,
                'synonym_key_id': synonym_key_id,
                'date_create': datetime.now(),
            })
            connection.execute(ins)
        m = "DataProvider(name='{}', type_name='{}', properties='{}', " \
            "synonym_key='{}'): Data provider had been successfully" \
            " registered!'"
        LOGGER.debug(
            m.format(name, cls.get_type_name(), properties, synonym_key)
        )
        if return_object:
            return cls(name, *args, **kwargs)

    @classmethod
    def update_data_provider(cls, current_name, *args, new_name=None,
                             properties={}, synonym_key=None,
                             return_object=True, **kwargs):
        if new_name is None:
            new_name = current_name
        m = "DataProvider(current_name='{}', new_name='{}', type_name='{}'," \
            "properties='{}', synonym_key='{}'): updating data provider...'"
        LOGGER.debug(
            m.format(
                current_name, new_name, cls.get_type_name(),
                properties, synonym_key
            )
        )
        with Connector.get_connection() as connection:
            cls.assert_data_provider_exists(current_name, bind=connection)
            synonym_key_id = None
            if synonym_key is not None:
                synonym_key_id = TaxonomyManager.get_synonym_key(
                    synonym_key,
                    bind=connection
                )['id']
            upd = niamoto_db_meta.data_provider.update().values({
                'name': new_name,
                'properties': properties,
                'synonym_key_id': synonym_key_id,
                'date_update': datetime.now(),
            }).where(niamoto_db_meta.data_provider.c.name == current_name)
            connection.execute(upd)
        m = "DataProvider(current_name='{}', new_name='{}', type_name='{}'," \
            " properties='{}', synonym_key='{}'): Data provider had been " \
            "successfully updated!'"
        LOGGER.debug(
            m.format(
                current_name, new_name, cls.get_type_name(),
                properties, synonym_key)
        )
        if return_object:
            return cls(new_name, *args, **kwargs)

    @classmethod
    def unregister_data_provider(cls, name, bind=None):
        """
        Unregister a data provider from the database.
        :param name: The name of the data provider to unregister.
        """
        cls.assert_data_provider_exists(name)
        delete_stmt = niamoto_db_meta.data_provider.delete().where(
            niamoto_db_meta.data_provider.c.name == name
        )
        if bind is not None:
            bind.execute(delete_stmt)
            return
        with Connector.get_connection() as connection:
            with connection.begin():
                connection.execute(delete_stmt)

    @staticmethod
    def assert_data_provider_does_not_exist(name, bind=None):
        sel = niamoto_db_meta.data_provider.select().where(
            niamoto_db_meta.data_provider.c.name == name
        )
        if bind is not None:
            r = bind.execute(sel).rowcount
        else:
            with Connector.get_connection() as connection:
                r = connection.execute(sel).rowcount
        if r > 0:
            m = "The data provider '{}' already exists in database."
            raise RecordAlreadyExistsError(m.format(name))

    @staticmethod
    def assert_data_provider_exists(name, bind=None):
        sel = niamoto_db_meta.data_provider.select().where(
            niamoto_db_meta.data_provider.c.name == name
        )
        if bind is not None:
            r = bind.execute(sel).rowcount
        else:
            with Connector.get_connection() as connection:
                r = connection.execute(sel).rowcount
        if r == 0:
            m = "The data provider '{}' does not exist in database."
            raise NoRecordFoundError(m.format(name))
