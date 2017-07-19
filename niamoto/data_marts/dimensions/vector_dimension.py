# coding: utf-8

import geopandas as gpd

from niamoto.conf import settings
from niamoto.db.connector import Connector
from niamoto.data_marts.dimensions.base_dimension import BaseDimension
from niamoto.vector.vector_manager import VectorManager
from niamoto.data_publishers.vector_publisher import VectorDataPublisher


class VectorDimension(BaseDimension):
    """
    Dimension extracted from a registered vector layer.
    """

    def __init__(self, vector_name):
        self.vector_name = vector_name
        pk_cols = VectorManager.get_vector_primary_key_columns(vector_name)
        self.geom_col = VectorManager.get_geometry_column(vector_name)
        self.pk_cols = [i[0] for i in pk_cols]
        tbl = VectorManager.get_vector_sqlalchemy_table(vector_name)
        columns = [c.copy() for c in tbl.columns if c.name not in self.pk_cols]
        super(VectorDimension, self).__init__(
            vector_name,
            columns,
            publisher=VectorDataPublisher()
        )

    @classmethod
    def load(cls, dimension_name):
        return cls(dimension_name)

    def get_values(self):
        """
        :return: A dataframe containing the values stored in database for
            the dimension.
        """
        return VectorManager.get_vector_geo_dataframe(self.name)

    def populate_from_publisher(self, *args, **kwargs):
        return super(VectorDimension, self).populate_from_publisher(
            self.vector_name,
            *args,
            **kwargs
        )

    def populate(self, dataframe):
        geom_col_name = self.geom_col[0]
        srid = self.geom_col[2]
        dataframe[geom_col_name] = dataframe[geom_col_name].apply(
            lambda x: "SRID={};{}".format(srid, x)
        )
        return super(VectorDimension, self).populate(dataframe)

    @classmethod
    def get_description(cls):
        return "Vector dimension."

    @classmethod
    def get_key(cls):
        return "VECTOR_DIMENSION"
