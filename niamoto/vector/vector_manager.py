# coding: utf-8

from datetime import datetime
import subprocess
import os

from sqlalchemy import select
import pandas as pd

from niamoto.db.connector import Connector
from niamoto.conf import settings
from niamoto.db import metadata as meta
from niamoto.log import get_logger
from niamoto.exceptions import NoRecordFoundError, RecordAlreadyExistsError


LOGGER = get_logger(__name__)


class VectorManager:
    """
    Class managing the vector registry (list, add, update, delete).
    """

    @classmethod
    def get_vector_list(cls):
        """
        :return: A pandas DataFrame containing all the vector entries
        available within the given database.
        """
        with Connector.get_connection() as connection:
            sel = select([meta.vector_registry])
            return pd.read_sql(
                sel,
                connection,
                index_col=meta.vector_registry.c.id.name
            )

    @classmethod
    def add_vector(cls, vector_file_path, name):
        """
        Add a vector in database and register it the Niamoto vector registry.
        Uses ogr2ogr. All vectors are stored in the
        settings.NIAMOTO_RASTER_SCHEMA schema.
        :param vector_file_path: The path to the vector file.
        :param name: The name of the vector. The created table will have this
        name.
        """
        LOGGER.debug("VectorManager.add_vector({}, {})".format(
            vector_file_path,
            name
        ))
        if not os.path.exists(vector_file_path):
            raise FileNotFoundError(
                "The vector {} does not exist".format(vector_file_path)
            )
        cls.assert_vector_does_not_exist(name)
        pg_str = "dbname='{}' host='{}' port='{}' user='{}' password='{}'"
        pg_str = pg_str.format(
            settings.NIAMOTO_DATABASE["NAME"],
            settings.NIAMOTO_DATABASE["HOST"],
            settings.NIAMOTO_DATABASE["PORT"],
            settings.NIAMOTO_DATABASE["USER"],
            settings.NIAMOTO_DATABASE["NAME"],
        )
        p = subprocess.Popen([
            "ogr2ogr",
            "-overwrite",
            '-f', 'PostgreSQL', 'PG:{}'.format(pg_str),
            '-nln', '{}.{}'.format(settings.NIAMOTO_VECTOR_SCHEMA, name),
            '-nlt', 'PROMOTE_TO_MULTI',
            vector_file_path,
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if stdout:
            LOGGER.debug(stdout)
        if stderr:
            LOGGER.debug(stderr)
        if p.returncode != 0:
            raise RuntimeError("vector import failed.")
        ins = meta.vector_registry.insert().values({
            'name': name,
            'date_create': datetime.now(),
        })
        with Connector.get_connection() as connection:
            connection.execute(ins)

    @classmethod
    def update_vector(cls, vector_file_path, name, new_name=None):
        """
        Update an existing vector in database and update it the Niamoto
        vector registry. Uses ogr2ogr. All vectors are stored in the
        settings.NIAMOTO_RASTER_SCHEMA schema.
        :param vector_file_path: The path to the vector file.
        :param name: The name of the vector.
        :param new_name: The new name of the vector (not changed if None).
        """
        LOGGER.debug(
            "VectorManager.update_vector({}, {}, new_name={})".format(
                vector_file_path,
                name,
                new_name
            )
        )
        if not os.path.exists(vector_file_path):
            raise FileNotFoundError(
                "The vector {} does not exist".format(vector_file_path)
            )
        cls.assert_vector_exists(name)
        if new_name is None:
            new_name = name
        else:
            cls.assert_vector_does_not_exist(new_name)
        with Connector.get_connection() as connection:
            connection.execute("DROP TABLE IF EXISTS {};".format(
                "{}.{}".format(settings.NIAMOTO_VECTOR_SCHEMA, name)
            ))
        tb = "{}.{}".format(settings.NIAMOTO_VECTOR_SCHEMA, new_name)
        pg_str = "dbname='{}' host='{}' port='{}' user='{}' password='{}'"
        pg_str = pg_str.format(
            settings.NIAMOTO_DATABASE["NAME"],
            settings.NIAMOTO_DATABASE["HOST"],
            settings.NIAMOTO_DATABASE["PORT"],
            settings.NIAMOTO_DATABASE["USER"],
            settings.NIAMOTO_DATABASE["NAME"],
        )
        p = subprocess.Popen([
            "ogr2ogr",
            "-overwrite",
            "-lco", "SCHEMA={}".format(settings.NIAMOTO_VECTOR_SCHEMA),
            "-lco", "OVERWRITE=YES",
            '-f', 'PostgreSQL', 'PG:{}'.format(pg_str),
            '-nln', tb,
            '-nlt', 'PROMOTE_TO_MULTI',
            vector_file_path,
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if stdout:
            LOGGER.debug(str(stdout))
        if stderr:
            LOGGER.debug(str(stderr))
        if p.returncode != 0:
            raise RuntimeError("vector import failed.")
        upd = meta.vector_registry.update().values({
            'name': new_name,
            'date_update': datetime.now(),
        }).where(meta.vector_registry.c.name == name)
        with Connector.get_connection() as connection:
            if new_name != name:
                connection.execute(upd)

    @classmethod
    def delete_vector(cls, name, connection=None):
        """
        Delete an existing vector.
        :param name: The name of the vector.
        :param connection: If provided, use an existing connection.
        """
        LOGGER.debug("VectorManager.delete_vector(connection={})".format(
            str(connection)
        ))
        cls.assert_vector_exists(name)
        close_after = False
        if connection is None:
            close_after = True
            connection = Connector.get_engine().connect()
        with connection.begin():
            connection.execute("DROP TABLE IF EXISTS {};".format(
                "{}.{}".format(settings.NIAMOTO_VECTOR_SCHEMA, name)
            ))
            del_stmt = meta.vector_registry.delete().where(
                meta.vector_registry.c.name == name
            )
            connection.execute(del_stmt)
        if close_after:
            connection.close()

    @staticmethod
    def assert_vector_does_not_exist(name):
        sel = meta.vector_registry.select().where(
            meta.vector_registry.c.name == name
        )
        with Connector.get_connection() as connection:
            r = connection.execute(sel).rowcount
            if r > 0:
                m = "The vector '{}' already exists in database."
                raise RecordAlreadyExistsError(m.format(name))

    @staticmethod
    def assert_vector_exists(name, connection=None):
        sel = meta.vector_registry.select().where(
            meta.vector_registry.c.name == name
        )
        if connection is not None:
            r = connection.execute(sel).rowcount
        else:
            with Connector.get_connection() as connection:
                r = connection.execute(sel).rowcount
        if r == 0:
            m = "The vector '{}' does not exist in database."
            raise NoRecordFoundError(m.format(name))
