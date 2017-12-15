# coding: utf-8

import os
import subprocess
from datetime import datetime

from sqlalchemy import *
import pandas as pd
import rasterio

from niamoto.db import metadata as niamoto_db_meta
from niamoto.db.connector import Connector
from niamoto.conf import settings
from niamoto.exceptions import NoRecordFoundError, RecordAlreadyExistsError, \
    IncoherentDatabaseStateError
from niamoto.log import get_logger, LOG_FILE


LOGGER = get_logger(__name__)


class RasterManager:
    """
    Class managing the raster registry (list, add, update, delete).
    """

    REGISTRY_TABLE = niamoto_db_meta.raster_registry
    DB_SCHEMA = settings.NIAMOTO_RASTER_SCHEMA

    @classmethod
    def get_raster_list(cls, alt_index=None):
        """
        :param alt_index: Specify an alternative index.
        :return: A pandas DataFrame containing all the raster entries
        available within the given database.
        """
        if alt_index is not None:
            index = alt_index
        else:
            index = cls.REGISTRY_TABLE.c.id.name
        with Connector.get_connection() as connection:
            sel = select([cls.REGISTRY_TABLE])
            return pd.read_sql(
                sel,
                connection,
                index_col=index
            )

    @classmethod
    def add_raster(cls, name, raster_file_path, tile_dimension=None,
                   register=False, properties={}, **kwargs):
        """
        Add a raster in database and register it the Niamoto raster registry.
        Uses raster2pgsql command. The raster is cut in tiles, using the
        dimension tile_width x tile_width. All rasters
        are stored in the settings.NIAMOTO_RASTER_SCHEMA schema. c.f.
        https://postgis.net/docs/using_raster_dataman.html#RT_Raster_Loader
        for more details on raster2pgsql.
        :param name: The name of the raster.
        :param raster_file_path: The path to the raster file.
        :param tile_dimension: The tile dimension (width, height), if None,
            tile dimension will be chosen automatically by PostGIS.
        :param register: Register the raster as a filesystem (out-db) raster.
            (-R option of raster2pgsql).
        :param properties: A dict of arbitrary properties.
        """
        if not os.path.exists(raster_file_path):
            raise FileNotFoundError(
                "The raster {} does not exist".format(raster_file_path)
            )
        cls.assert_raster_does_not_exist(name)
        cls.assert_raster_schema_exists()
        if tile_dimension is not None:
            dim = "{}x{}".format(tile_dimension[0], tile_dimension[1])
        else:
            dim = 'auto'
        tb = "{}.{}".format(cls.DB_SCHEMA, name)
        os.environ["PGPASSWORD"] = settings.NIAMOTO_DATABASE["PASSWORD"]
        raster2pgsql_args = [
            "raster2pgsql", "-c", "-Y", '-C', '-t', dim,
            '-I', '-M', raster_file_path, tb,
        ]
        if register:
            raster2pgsql_args.append('-R')
        p1 = subprocess.Popen(
            raster2pgsql_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        with open(LOG_FILE, mode='a') as log_file:
            p2 = subprocess.call([
                "psql",
                "-q",
                "-U", settings.NIAMOTO_DATABASE["USER"],
                "-h", settings.NIAMOTO_DATABASE["HOST"],
                "-p", settings.NIAMOTO_DATABASE["PORT"],
                "-d", settings.NIAMOTO_DATABASE["NAME"],
                "-w",
            ], stdin=p1.stdout, stdout=log_file, stderr=log_file)
            stdout, stderr = p1.communicate()
        if stderr:
            LOGGER.debug(stderr)
        if stdout:
            LOGGER.debug(stdout)
        os.environ["PGPASSWORD"] = ""
        if p2 != 0 or p1.returncode != 0:
            raise RuntimeError(
                "raster import failed, check the logs for more details."
            )
        values = {
            'name': name,
            'date_create': datetime.now(),
            'properties': properties,
        }
        values.update(kwargs)
        ins = cls.REGISTRY_TABLE.insert().values(values)
        with Connector.get_connection() as connection:
            connection.execute(ins)

    @classmethod
    def update_raster(cls, name, raster_file_path=None, new_name=None,
                      tile_dimension=None, register=False, properties=None):
        """
        Update an existing raster in database and update it the Niamoto
        raster registry. Uses raster2pgsql command. The raster is cut in
        tiles, using the dimension tile_width x tile_width. All rasters
        are stored in the settings.NIAMOTO_RASTER_SCHEMA schema. c.f.
        https://postgis.net/docs/using_raster_dataman.html#RT_Raster_Loader
        for more details on raster2pgsql.
        :param name: The name of the raster. If None, the raster data won't
            be updated.
        :param raster_file_path: The path to the raster file.
        :param new_name: The new name of the raster (not changed if None).
        :param tile_dimension: The tile dimension (width, height), if None,
            tile dimension will be chosen automatically by PostGIS.
        :param register: Register the raster as a filesystem (out-db) raster.
            (-R option of raster2pgsql).
        :param properties: A dict of arbitrary properties.
        """
        cls.assert_raster_exists(name)
        if new_name is None:
            new_name = name
        else:
            cls.assert_raster_does_not_exist(new_name)
        if raster_file_path is not None:
            if not os.path.exists(raster_file_path):
                raise FileNotFoundError(
                    "The raster {} does not exist".format(raster_file_path)
                )
            if tile_dimension is not None:
                dim = "{}x{}".format(tile_dimension[0], tile_dimension[1])
            else:
                dim = 'auto'
            os.environ["PGPASSWORD"] = settings.NIAMOTO_DATABASE["PASSWORD"]
            d = "-d"
            if new_name != name:
                d = "-c"
            tb = "{}.{}".format(cls.DB_SCHEMA, new_name)
            raster2pgsql_args = [
                "raster2pgsql", d, "-C", "-Y", '-t', dim,
                '-I', '-M', raster_file_path, tb,
            ]
            if register:
                raster2pgsql_args.append('-R')
            p1 = subprocess.Popen(
                raster2pgsql_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            with open(LOG_FILE, mode='a') as log_file:
                p2 = subprocess.call([
                    "psql",
                    "-q",
                    "-U", settings.NIAMOTO_DATABASE["USER"],
                    "-h", settings.NIAMOTO_DATABASE["HOST"],
                    "-p", settings.NIAMOTO_DATABASE["PORT"],
                    "-d", settings.NIAMOTO_DATABASE["NAME"],
                    "-w",
                ], stdin=p1.stdout, stdout=log_file, stderr=log_file)
                stdout, stderr = p1.communicate()
            if stderr:
                LOGGER.debug(stderr)
            os.environ["PGPASSWORD"] = ""
            if p2 != 0 or p1.returncode != 0:
                raise RuntimeError("raster import failed.")
        upd_values = {
            'name': new_name,
            'date_update': datetime.now()
        }
        if properties is not None:
            upd_values['properties'] = properties
        upd = cls.REGISTRY_TABLE.update() \
            .values(upd_values)\
            .where(cls.REGISTRY_TABLE.c.name == name)
        with Connector.get_connection() as connection:
            connection.execute(upd)
            if new_name != name:
                if raster_file_path is not None:
                    connection.execute(
                        "DROP TABLE IF EXISTS {};".format(
                            "{}.{}".format(
                                cls.DB_SCHEMA,
                                name
                            )
                        )
                    )
                else:
                    connection.execute(
                        "ALTER TABLE {} RENAME TO {};".format(
                            '{}.{}'.format(
                                cls.DB_SCHEMA,
                                name
                            ),
                            '{}.{}'.format(
                                cls.DB_SCHEMA,
                                new_name
                            )
                        )
                    )

    @classmethod
    def delete_raster(cls, name, connection=None):
        """
        Delete an existing raster.
        :param name: The name of the raster.
        :param connection: If provided, use an existing connection.
        """
        cls.assert_raster_exists(name)
        close_after = False
        if connection is None:
            close_after = True
            connection = Connector.get_engine().connect()
        with connection.begin():
            connection.execute("DROP TABLE IF EXISTS {};".format(
                "{}.{}".format(cls.DB_SCHEMA, name)
            ))
            del_stmt = cls.REGISTRY_TABLE.delete().where(
                cls.REGISTRY_TABLE.c.name == name
            )
            connection.execute(del_stmt)
        if close_after:
            connection.close()

    @classmethod
    def get_raster_srid(cls, raster_file_path):
        if not os.path.exists(raster_file_path):
            raise FileNotFoundError(
                "The raster '{}' does not exist".format(raster_file_path)
            )
        raster = rasterio.open(raster_file_path)
        srid = int(raster.crs['init'].split('epsg:')[1])
        raster.close()
        return srid

    @classmethod
    def assert_raster_does_not_exist(cls, name):
        sel = cls.REGISTRY_TABLE.select().where(
            cls.REGISTRY_TABLE.c.name == name
        )
        with Connector.get_connection() as connection:
            r = connection.execute(sel).rowcount
            if r > 0:
                m = "The raster '{}' already exists in database."
                raise RecordAlreadyExistsError(m.format(name))

    @classmethod
    def assert_raster_exists(cls, name, connection=None):
        sel = cls.REGISTRY_TABLE.select().where(
            cls.REGISTRY_TABLE.c.name == name
        )
        if connection is not None:
            r = connection.execute(sel).rowcount
        else:
            with Connector.get_connection() as connection:
                r = connection.execute(sel).rowcount
        if r == 0:
            m = "The raster '{}' does not exist in database."
            raise NoRecordFoundError(m.format(name))

    @classmethod
    def assert_raster_schema_exists(cls, connection=None):
        sel = \
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name= '{}'
            """.format(cls.DB_SCHEMA)
        if connection is not None:
            r = connection.execute(sel).rowcount
        else:
            with Connector.get_connection() as connection:
                r = connection.execute(sel).rowcount
        if r == 0:
            m = "The schema '{}' does not exists in database."
            raise IncoherentDatabaseStateError(m.format(cls.DB_SCHEMA))
