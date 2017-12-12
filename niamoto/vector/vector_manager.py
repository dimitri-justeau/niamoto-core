# coding: utf-8

from datetime import datetime
import subprocess
import os

from sqlalchemy import select, MetaData
import pandas as pd
import geopandas as gpd

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
    def add_vector(cls, name, vector_file_path, properties={}):
        """
        Add a vector in database and register it the Niamoto vector registry.
        Uses ogr2ogr. All vectors are stored in the
        settings.NIAMOTO_RASTER_SCHEMA schema.
        :param name: The name of the vector. The created table will have this
        name.
        :param vector_file_path: The path to the vector file.
        :param properties: A dict of arbitrary properties.
        """
        LOGGER.debug("VectorManager.add_vector({}, {})".format(
            vector_file_path,
            name
        ))
        cls.assert_vector_does_not_exist(name)
        if not os.path.exists(vector_file_path):
            raise FileNotFoundError(
                "The file {} does not exist.".format(vector_file_path)
            )
        pg_str = "dbname='{}' host='{}' port='{}' user='{}' password='{}'"
        pg_str = pg_str.format(
            settings.NIAMOTO_DATABASE["NAME"],
            settings.NIAMOTO_DATABASE["HOST"],
            settings.NIAMOTO_DATABASE["PORT"],
            settings.NIAMOTO_DATABASE["USER"],
            settings.NIAMOTO_DATABASE["PASSWORD"],
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
            'properties': properties,
        })
        with Connector.get_connection() as connection:
            connection.execute(ins)

    @classmethod
    def update_vector(cls, name, vector_file_path=None, new_name=None,
                      properties=None):
        """
        Update an existing vector in database and update it the Niamoto
        vector registry. Uses ogr2ogr. All vectors are stored in the
        settings.NIAMOTO_RASTER_SCHEMA schema.
        :param name: The name of the vector.
        :param vector_file_path: The path to the vector file. If None, the
            vector data won't be updated.
        :param new_name: The new name of the vector (not changed if None).
        :param properties: A dict of arbitrary properties.
        """
        LOGGER.debug(
            "VectorManager.update_vector({}, {}, new_name={})".format(
                name,
                vector_file_path,
                new_name
            )
        )
        cls.assert_vector_exists(name)
        if new_name is None:
            new_name = name
        else:
            cls.assert_vector_does_not_exist(new_name)
        if vector_file_path is not None:
            if not os.path.exists(vector_file_path):
                raise FileNotFoundError(
                    "The vector {} does not exist".format(vector_file_path)
                )
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
                settings.NIAMOTO_DATABASE["PASSWORD"],
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
        upd_values = {
            'name': new_name,
            'date_update': datetime.now()
        }
        if properties is not None:
            upd_values['properties'] = properties
        upd = meta.vector_registry.update() \
            .values(upd_values)\
            .where(meta.vector_registry.c.name == name)
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

    @classmethod
    def get_geometry_column(cls, vector_name):
        """
        Find the geometry column of a raster, inspecting the geometry_columns
        table. Assume that there is a single geometry column, if several are
        queried return the first one.
        :return: The geometry column name, type and srid (name, type, srid).
        """
        with Connector.get_connection() as connection:
            cls.assert_vector_exists(vector_name, connection)
            sql = \
                """
                SELECT f_geometry_column,
                    type,
                    srid
                FROM public.geometry_columns
                WHERE f_table_schema = '{}'
                    AND f_table_name = '{}'
                LIMIT 1;
                """.format(
                    settings.NIAMOTO_VECTOR_SCHEMA,
                    vector_name
                )
            result = connection.execute(sql)
            return result.fetchone()

    @classmethod
    def get_vector_primary_key_columns(cls, vector_name):
        with Connector.get_connection() as connection:
            cls.assert_vector_exists(vector_name, connection)
            sql = \
                """
                SELECT a.attname,
                    format_type(a.atttypid, a.atttypmod) AS data_type
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{}.{}'::regclass
                    AND i.indisprimary;
                """.format(
                    settings.NIAMOTO_VECTOR_SCHEMA,
                    vector_name,
                )
            result = connection.execute(sql)
            return result.fetchall()

    @classmethod
    def get_vector_geo_dataframe(cls, vector_name, geojson_filter=None,
                                 geojson_cut=False):
        """
        Return a registered vector as a GeoDataFrame.
        :param vector_name: The name of the vector.
        :param geojson_filter: Optional: if specified (as str), will only
            return the features intersecting with the geojson.
        :param geojson_cut: If True, return the intersection with the geojson
            filter (cut the intersecting features).
        :return: A GeoDataFrame corresponding to the vector.
        """
        geom_col = cls.get_geometry_column(vector_name)
        pk_cols = cls.get_vector_primary_key_columns(vector_name)
        cols = cls.get_vector_sqlalchemy_table(vector_name).columns
        cols_str = [c.name for c in cols]
        where_statement = ''
        if geojson_filter is not None:
            geojson_postgis = \
                """
                    ST_Transform(
                        ST_SetSRID(
                            ST_MakeValid(ST_GeomFromGeoJSON('{}')),
                            4326
                        ),
                        {}
                    )
                """.format(geojson_filter, geom_col[2])
            where_statement = \
                """
                    WHERE ST_Intersects(
                        {},
                        {}
                    )
                """.format(
                    geom_col[0],
                    geojson_postgis
                )
            if geojson_cut:
                cols_str = []
                for col in cols:
                    if col.name == geom_col[0]:
                        cols_str.append(
                            "ST_Intersection({}, {}) AS {}".format(
                                geom_col[0],
                                geojson_postgis,
                                col.name,
                            )
                        )
                    else:
                        cols_str.append(col.name)
        with Connector.get_connection() as connection:
            sql = "SELECT {} FROM {}.{} {};".format(
                ','.join(cols_str),
                settings.NIAMOTO_VECTOR_SCHEMA,
                vector_name,
                where_statement,
            )
            return gpd.read_postgis(
                sql,
                connection,
                index_col=[i[0] for i in pk_cols],
                geom_col=geom_col[0],
                crs='+init=epgs:{}'.format(geom_col[2]),
            )

    @classmethod
    def get_vector_sqlalchemy_table(cls, vector_name):
        """
        Inspect the vector table and return a SQLAlchemy
        Table metadata object.
        :param vector_name: The vector to inspect.
        :return: A SQLAlchemy Table metadata object.
        """
        with Connector.get_connection() as connection:
            cls.assert_vector_exists(vector_name, connection)
            meta_ = MetaData()
            meta_.reflect(
                bind=connection,
                schema=settings.NIAMOTO_VECTOR_SCHEMA,
            )
            table_key = '{}.{}'.format(
                settings.NIAMOTO_VECTOR_SCHEMA,
                vector_name,
            )
            return meta_.tables[table_key]
