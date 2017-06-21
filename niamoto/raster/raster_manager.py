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
from niamoto.exceptions import NoRecordFoundError, RecordAlreadyExistsError
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


class RasterManager:
    """
    Class managing the raster registry (list, add, update, delete).
    """

    @classmethod
    def get_raster_list(cls):
        """
        :return: A pandas DataFrame containing all the raster entries
        available within the given database.
        """
        with Connector.get_connection() as connection:
            sel = select([niamoto_db_meta.raster_registry])
            return pd.read_sql(
                sel,
                connection,
                index_col=niamoto_db_meta.raster_registry.c.id.name
            )

    @classmethod
    def add_raster(cls, raster_file_path, name, tile_dimension=None):
        """
        Add a raster in database and register it the Niamoto raster registry.
        Uses raster2pgsql command. The raster is cut in tiles, using the
        dimension tile_width x tile_width. All rasters
        are stored in the settings.NIAMOTO_RASTER_SCHEMA schema.
        :param raster_file_path: The path to the raster file.
        :param name: The name of the raster.
        :param tile_dimension: The tile dimension (width, height), if None,
            tile dimension will be chosen automatically by PostGIS.
        """
        if not os.path.exists(raster_file_path):
            raise FileNotFoundError(
                "The raster {} does not exist".format(raster_file_path)
            )
        cls.assert_raster_does_not_exist(name)
        if tile_dimension is not None:
            dim = "{}x{}".format(tile_dimension[0], tile_dimension[1])
        else:
            dim = 'auto'
        tb = "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, name)
        os.environ["PGPASSWORD"] = settings.NIAMOTO_DATABASE["PASSWORD"]
        p1 = subprocess.Popen([
            "raster2pgsql", "-c", '-t', dim, '-I', raster_file_path, tb,
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p2 = subprocess.call([
            "psql",
            "-q",
            "-U", settings.NIAMOTO_DATABASE["USER"],
            "-h", settings.NIAMOTO_DATABASE["HOST"],
            "-p", settings.NIAMOTO_DATABASE["PORT"],
            "-d", settings.NIAMOTO_DATABASE["NAME"],
            "-w",
        ], stdin=p1.stdout)
        stdout, stderr = p1.communicate()
        if stderr:
            LOGGER.debug(stderr)
        os.environ["PGPASSWORD"] = ""
        if p2 != 0 or p1.returncode != 0:
            raise RuntimeError(
                "raster import failed, check the logs for more details."
            )
        ins = niamoto_db_meta.raster_registry.insert().values({
            'name': name,
            'date_create': datetime.now(),
        })
        with Connector.get_connection() as connection:
            connection.execute(ins)

    @classmethod
    def update_raster(cls, raster_file_path, name, new_name=None,
                      tile_dimension=None):
        """
        Update an existing raster in database and update it the Niamoto
        raster registry. Uses raster2pgsql command. The raster is cut in
        tiles, using the dimension tile_width x tile_width. All rasters
        are stored in the settings.NIAMOTO_RASTER_SCHEMA schema.
        :param raster_file_path: The path to the raster file.
        :param name: The name of the raster.
        :param new_name: The new name of the raster (not changed if None).
        :param tile_dimension: The tile dimension (width, height), if None,
            tile dimension will be chosen automatically by PostGIS.
        """
        if not os.path.exists(raster_file_path):
            raise FileNotFoundError(
                "The raster {} does not exist".format(raster_file_path)
            )
        cls.assert_raster_exists(name)
        if tile_dimension is not None:
            dim = "{}x{}".format(tile_dimension[0], tile_dimension[1])
        else:
            dim = 'auto'
        os.environ["PGPASSWORD"] = settings.NIAMOTO_DATABASE["PASSWORD"]
        dev_null = open(os.devnull, 'w')  # Force quiet
        d = "-d"
        if new_name is None:
            new_name = name
        else:
            cls.assert_raster_does_not_exist(new_name)
            d = "-c"
        tb = "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, new_name)
        p1 = subprocess.Popen([
            "raster2pgsql", d, '-t', dim, '-I', raster_file_path, tb,
        ], stdout=subprocess.PIPE, stderr=dev_null)
        dev_null.close()
        p2 = subprocess.call([
            "psql",
            "-q",
            "-U", settings.NIAMOTO_DATABASE["USER"],
            "-h", settings.NIAMOTO_DATABASE["HOST"],
            "-p", settings.NIAMOTO_DATABASE["PORT"],
            "-d", settings.NIAMOTO_DATABASE["NAME"],
            "-w",
        ], stdin=p1.stdout)
        p1.communicate()
        os.environ["PGPASSWORD"] = ""
        if p2 != 0:
            raise RuntimeError("raster import failed.")
        upd = niamoto_db_meta.raster_registry.update().values({
            'name': new_name,
            'date_update': datetime.now(),
        }).where(niamoto_db_meta.raster_registry.c.name == name)
        with Connector.get_connection() as connection:
            connection.execute(upd)
            if new_name != name:
                connection.execute("DROP TABLE IF EXISTS {};".format(
                    "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, name)
                ))

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
                "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, name)
            ))
            del_stmt = niamoto_db_meta.raster_registry.delete().where(
                niamoto_db_meta.raster_registry.c.name == name
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

    @staticmethod
    def assert_raster_does_not_exist(name):
        sel = niamoto_db_meta.raster_registry.select().where(
            niamoto_db_meta.raster_registry.c.name == name
        )
        with Connector.get_connection() as connection:
            r = connection.execute(sel).rowcount
            if r > 0:
                m = "The raster '{}' already exists in database."
                raise RecordAlreadyExistsError(m.format(name))

    @staticmethod
    def assert_raster_exists(name, connection=None):
        sel = niamoto_db_meta.raster_registry.select().where(
            niamoto_db_meta.raster_registry.c.name == name
        )
        if connection is not None:
            r = connection.execute(sel).rowcount
        else:
            with Connector.get_connection() as connection:
                r = connection.execute(sel).rowcount
        if r == 0:
            m = "The raster '{}' does not exist in database."
            raise NoRecordFoundError(m.format(name))
