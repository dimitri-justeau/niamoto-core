# coding: utf-8

from niamoto.data_marts.dimensions.base_dimension import BaseDimension
from niamoto.vector.vector_manager import VectorManager
from niamoto.data_publishers.vector_publisher import VectorDataPublisher


class VectorDimension(BaseDimension):
    """
    Dimension extracted from a registered vector layer.
    """

    def __init__(self, vector_name, label_col='label'):
        self.vector_name = vector_name
        pk_cols = VectorManager.get_vector_primary_key_columns(vector_name)
        self.geom_col = VectorManager.get_geometry_column(vector_name)
        self.pk_cols = [i[0] for i in pk_cols]
        tbl = VectorManager.get_vector_sqlalchemy_table(vector_name)
        columns = [c.copy() for c in tbl.columns if c.name not in self.pk_cols]
        super(VectorDimension, self).__init__(
            vector_name,
            columns,
            publisher=VectorDataPublisher(),
            label_col=label_col,
        )

    @property
    def geom_column_name(self):
        return self.geom_col[0]

    @property
    def geom_type(self):
        return self.geom_col[1]

    @property
    def srid(self):
        return self.geom_col[2]

    @classmethod
    def load(cls, dimension_name, label_col='label', properties={}):
        return cls(dimension_name, label_col)

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

    def populate(self, dataframe, *args, **kwargs):
        geom_col_name = self.geom_column_name
        srid = self.srid
        dataframe[geom_col_name] = dataframe[geom_col_name].apply(
            lambda x: "SRID={};{}".format(srid, x)
        )
        return super(VectorDimension, self).populate(
            dataframe,
            *args,
            **kwargs
        )

    @classmethod
    def get_description(cls):
        return "Vector dimension."

    @classmethod
    def get_key(cls):
        return "VECTOR_DIMENSION"
