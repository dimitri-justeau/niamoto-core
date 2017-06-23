# coding: utf-8

from sqlalchemy import select, func
import pandas as pd

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector


class PlotDataPublisher(BaseDataPublisher):
    """
    Publish plot dataframe.
    """

    @classmethod
    def get_key(cls):
        return 'plots'

    @classmethod
    def get_description(cls):
        return "Publish the plot dataframe with properties as columns."

    def _process(self, *args, properties=None, **kwargs):
        """
        :param properties: List of properties to retain. Can be a python list
            or a comma (',') separated string.
        """
        with Connector.get_connection() as connection:
            sel_keys = select([
                func.jsonb_object_keys(
                    meta.plot.c.properties
                ).distinct(),
            ])
            if properties is None:
                keys = [i[0] for i in connection.execute(sel_keys).fetchall()]
            else:
                if isinstance(properties, str):
                    properties = properties.split(',')
                keys = properties
            props = [meta.plot.c.properties[k].label(k) for k in keys]
            sel = select([
                meta.plot.c.id.label('id'),
                meta.plot.c.name.label('name'),
                func.st_x(meta.plot.c.location).label('x'),
                func.st_y(meta.plot.c.location).label('y'),
            ] + props)
            df = pd.read_sql(sel, connection, index_col='id')
            #  Replace None values with nan
            df.fillna(value=pd.np.NAN, inplace=True)
            return df, [], {'index_label': 'id'}

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.SQL]

