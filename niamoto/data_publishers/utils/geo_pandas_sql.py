# coding: utf-8

import warnings

from pandas.core.dtypes.common import is_dict_like
from pandas import Series
from pandas.io.sql import SQLTable
from pandas.io.sql import SQLDatabase
from pandas.compat import text_type
from geopandas import GeoSeries, GeoDataFrame
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape
from shapely.geometry import *


class GeoSQLDatabase(SQLDatabase):

    def to_sql(self, frame, name, if_exists='fail', index=True,
               index_label=None, schema=None, chunksize=None, dtype=None):
        if dtype and not is_dict_like(dtype):
            dtype = {col_name: dtype for col_name in frame}
        if dtype is not None:
            from sqlalchemy.types import to_instance, TypeEngine
            for col, my_type in dtype.items():
                if not isinstance(to_instance(my_type), TypeEngine):
                    raise ValueError('The type of %s is not a SQLAlchemy '
                                     'type ' % col)
        table = GeoSQLTable(name, self, frame=frame, index=index,
                            if_exists=if_exists, index_label=index_label,
                            schema=schema, dtype=dtype)
        table.create()
        table.insert(chunksize)
        if not name.isdigit() and not name.islower():
            # check for potentially case sensitivity issues (GH7815)
            # Only check when name is not a number and name is not lower case
            engine = self.connectable.engine
            with self.connectable.connect() as conn:
                table_names = engine.table_names(
                    schema=schema or self.meta.schema,
                    connection=conn,
                )
            if name not in table_names:
                msg = (
                    "The provided table name '{0}' is not found exactly as "
                    "such in the database after writing the table, possibly "
                    "due to case sensitivity issues. Consider using lower "
                    "case table names."
                ).format(name)
                warnings.warn(msg, UserWarning)


class GeoSQLTable(SQLTable):
    """
    Extension of the pandas SQLTable for geometry columns.
    """

    def _get_column_names_and_types(self, dtype_mapper):
        column_names_and_types = []
        if self.index is not None:
            for i, idx_label in enumerate(self.index):
                idx_type = dtype_mapper(
                    self.frame.index._get_level_values(i))
                column_names_and_types.append((text_type(idx_label),
                                              idx_type, True))
        column_names_and_types += [
            (text_type(c),
             dtype_mapper(self.frame[c]),
             False)
            for c in self.frame.columns
        ]
        return column_names_and_types

    def _sqlalchemy_type(self, col):
        if isinstance(col, GeoSeries):
            srid = get_srid(col.crs)
            if all(isinstance(item, Point) or not item for item in col):
                return Geometry('POINT', srid)
            elif all(isinstance(item, LineString) or not item for item in col):
                return Geometry('LINESTRING', srid)
            elif all(isinstance(item, Polygon) or not item for item in col):
                return Geometry('POLYGON', srid)
            elif all(isinstance(item, MultiPoint) or not item for item in col):
                return Geometry('MULTIPOINT', srid)
            elif all(isinstance(item, MultiLineString) or not item for item in col):
                return Geometry('MULTILINESTRING', srid)
            elif all(isinstance(item, MultiPolygon) or not item for item in col):
                return Geometry('MULTIPOLYGON', srid)
            elif all(isinstance(item, GeometryCollection) or not item for item in col):
                return Geometry('GEOMETRYCOLLECTION', srid)
            else:
                return Geometry('GEOMETRY', srid)
        else:
            return super(GeoSQLTable, self)._sqlalchemy_type(col)

    def insert_data(self):
        column_names, data_list = super(GeoSQLTable, self).insert_data()
        geometry_column = column_names.index(self.frame._geometry_column_name)
        geometry = data_list[geometry_column]
        s = Series(geometry)
        srid = get_srid(self.frame.geometry.crs)
        s = s.apply(lambda x: from_shape(x, srid=srid))
        data_list[geometry_column] = s
        return column_names, data_list


def to_postgis(frame, name, con, schema=None, if_exists='fail',
               index=True, index_label=None, chunksize=None, dtype=None):
    if if_exists not in ('fail', 'replace', 'append'):
        raise ValueError("'{0}' is not valid for if_exists".format(if_exists))
    pandas_sql = GeoSQLDatabase(con, schema=schema)
    if isinstance(frame, GeoSeries):
        frame = GeoDataFrame({'geometry': frame}, geometry='geometry')
    elif not isinstance(frame, GeoDataFrame):
        raise NotImplementedError("'frame' argument should be either a "
                                  "GeoSeries or a GeoDataFrame")
    pandas_sql.to_sql(frame, name, if_exists=if_exists, index=index,
                      index_label=index_label, schema=schema,
                      chunksize=chunksize, dtype=dtype)


def get_srid(crs):
    srid = -1
    if isinstance(crs, dict):
        if 'init' in crs:
            s = crs['init'].split('epsg:')
            if len(s) > 0:
                srid = crs['init'].split('epsg:')[1]
    elif isinstance(crs, str):
        s = crs.split('epsg:')
        if len(s) > 0:
            srid = crs.split('epsg:')[1].split(' ')[0]
    return srid
