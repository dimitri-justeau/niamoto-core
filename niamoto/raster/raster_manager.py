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
from niamoto.exceptions import NoRecordFoundError, RecordAlreadyExists


class RasterManager:
    """
    Class managing the raster registry (list, add, update, delete).
    """

    @classmethod
    def get_raster_list(cls, database=settings.DEFAULT_DATABASE):
        """
        :param database: The database to work with.
        :return: A pandas DataFrame containing all the raster entries
        available within the given database.
        """
        with Connector.get_connection(database=database) as connection:
            sel = select([niamoto_db_meta.raster_registry])
            return pd.read_sql(
                sel,
                connection,
                index_col=niamoto_db_meta.raster_registry.c.name.name
            )

    @classmethod
    def add_raster(cls, raster_file_path, name, tile_width, tile_height,
                   srid=None, database=settings.DEFAULT_DATABASE):
        """
        Add a raster in database and register it the Niamoto raster registry.
        Uses raster2pgsql command. The raster is cut in tiles, using the
        dimension tile_width x tile_width. All rasters are stored
        :param raster_file_path: The path to the raster file.
        :param name: The name of the raster.
        :param tile_width: The tile width.
        :param tile_height: The tile height.
        :param srid: SRID to assign to stored raster. If None, use raster's
        metadata to determine which SRID to store.
        :param database: The database to store the raster.
        """
        if not os.path.exists(raster_file_path):
            raise FileNotFoundError(
                "The raster {} does not exist".format(raster_file_path)
            )
        cls._assert_raster_does_not_exist(name, database)
        if srid is None:
            srid = cls.get_raster_srid(raster_file_path)
        dim = "{}x{}".format(tile_width, tile_height)
        tb = "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, name)
        os.environ["PGPASSWORD"] = database["PASSWORD"]
        dev_null = open(os.devnull, 'w')  # Force quiet
        p1 = subprocess.Popen([
            "raster2pgsql", "-c", '-t', dim, '-I', raster_file_path, tb,
        ], stdout=subprocess.PIPE, stderr=dev_null)
        dev_null.close()
        p2 = subprocess.call([
            "psql",
            "-q",
            "-U", database["USER"],
            "-h", database["HOST"],
            "-p", database["PORT"],
            "-d", database["NAME"],
            "-w",
        ], stdin=p1.stdout)
        p1.communicate()
        os.environ["PGPASSWORD"] = ""
        if p2 != 0:
            raise RuntimeError("raster import failed.")
        ins = niamoto_db_meta.raster_registry.insert().values({
            'name': name,
            'tile_width': tile_width,
            'tile_height': tile_height,
            'srid': srid,
            'date_create': datetime.now(),
            'date_update': datetime.now(),
        })
        with Connector.get_connection(database=database) as connection:
            connection.execute(ins)

    @classmethod
    def update_raster(cls, raster_file_path, name, tile_width, tile_height,
                      srid=None, database=settings.DEFAULT_DATABASE):
        """
        Update an existing raster in database and register it the Niamoto
        raster registry. Uses raster2pgsql command. The raster is cut in
        tiles, using the dimension tile_width x tile_width. All rasters
        are stored
        :param raster_file_path: The path to the raster file.
        :param name: The name of the raster.
        :param tile_width: The tile width.
        :param tile_height: The tile height.
        :param srid: SRID to assign to stored raster. If None, use raster's
        metadata to determine which SRID to store.
        :param database: The database to store the raster.
        """
        if not os.path.exists(raster_file_path):
            raise FileNotFoundError(
                "The raster {} does not exist".format(raster_file_path)
            )
        cls._assert_raster_exists(name, database)
        if srid is None:
            srid = cls.get_raster_srid(raster_file_path)
        dim = "{}x{}".format(tile_width, tile_height)
        tb = "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, name)
        os.environ["PGPASSWORD"] = database["PASSWORD"]
        dev_null = open(os.devnull, 'w')  # Force quiet
        p1 = subprocess.Popen([
            "raster2pgsql", "-d", '-t', dim, '-I', raster_file_path, tb,
        ], stdout=subprocess.PIPE, stderr=dev_null)
        dev_null.close()
        p2 = subprocess.call([
            "psql",
            "-q",
            "-U", database["USER"],
            "-h", database["HOST"],
            "-p", database["PORT"],
            "-d", database["NAME"],
            "-w",
        ], stdin=p1.stdout)
        p1.communicate()
        os.environ["PGPASSWORD"] = ""
        if p2 != 0:
            raise RuntimeError("raster import failed.")
        upd = niamoto_db_meta.raster_registry.update().values({
            'tile_width': tile_width,
            'tile_height': tile_height,
            'srid': srid,
            'date_create': datetime.now(),
            'date_update': datetime.now(),
        }).where(niamoto_db_meta.raster_registry.c.name == name)
        with Connector.get_connection(database=database) as connection:
            connection.execute(upd)

    @classmethod
    def delete_raster(cls, name, database=settings.DEFAULT_DATABASE):
        """
        Delete an existing raster.
        :param name: The name of the raster.
        :param database: The database to delete the raster from.
        """
        cls._assert_raster_exists(name, database)
        with Connector.get_connection(database=database) as connection:
            with connection.begin():
                connection.execute("DROP TABLE IF EXISTS {};".format(
                    "{}.{}".format(settings.NIAMOTO_RASTER_SCHEMA, name)
                ))
                del_stmt = niamoto_db_meta.raster_registry.delete().where(
                    niamoto_db_meta.raster_registry.c.name == name
                )
                connection.execute(del_stmt)

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
    def _assert_raster_does_not_exist(name, database):
        # Check if the raster already exists
        sel = niamoto_db_meta.raster_registry.select().where(
            niamoto_db_meta.raster_registry.c.name == name
        )
        with Connector.get_connection(database=database) as connection:
            r = connection.execute(sel).rowcount
            if r > 0:
                m = "The raster '{}' already exists in database."
                raise RecordAlreadyExists(m.format(name))

    @staticmethod
    def _assert_raster_exists(name, database):
        # Check if the raster already exists
        sel = niamoto_db_meta.raster_registry.select().where(
            niamoto_db_meta.raster_registry.c.name == name
        )
        with Connector.get_connection(database=database) as connection:
            r = connection.execute(sel).rowcount
            if r == 0:
                m = "The raster '{}' does not exist in database."
                raise NoRecordFoundError(m.format(name))
