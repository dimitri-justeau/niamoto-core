# coding: utf-8

from sqlalchemy import select, func, cast, String
import pandas as pd

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector


class OccurrenceDataPublisher(BaseDataPublisher):
    """
    Publish occurrence dataframe.
    """

    @classmethod
    def get_key(cls):
        return 'occurrences'

    @classmethod
    def get_description(cls):
        return "Publish the occurrence dataframe with properties as columns."

    def _process(self, *args, properties=None, drop_null_properties=False,
                 **kwargs):
        """
        :param properties: List of properties to retain. Can be a python list
            or a comma (',') separated string.
        """
        with Connector.get_connection() as connection:
            sel_keys = select([
                func.jsonb_object_keys(
                    meta.occurrence.c.properties
                ).distinct(),
            ])
            if properties is None:
                keys = [i[0] for i in connection.execute(sel_keys).fetchall()]
            else:
                if isinstance(properties, str):
                    properties = properties.split(',')
                keys = properties
            props = [meta.occurrence.c.properties[k].label(k) for k in keys]
            sel = select([
                meta.occurrence.c.id.label('id'),
                meta.occurrence.c.taxon_id.label('taxon_id'),
                cast(meta.taxon.c.rank.label('rank'), String).label('rank'),
                meta.taxon.c.full_name.label('full_name'),
                func.st_x(meta.occurrence.c.location).label('x'),
                func.st_y(meta.occurrence.c.location).label('y'),
            ] + props).select_from(
                meta.occurrence.outerjoin(
                    meta.taxon,
                    meta.taxon.c.id == meta.occurrence.c.taxon_id
                )
            )
            df = pd.read_sql(sel, connection, index_col='id')
            df['taxon_id'] = df['taxon_id'].apply(pd.to_numeric)
            #  Replace None values with nan
            df.fillna(value=pd.np.NAN, inplace=True)
            if drop_null_properties:
                for k in keys:
                    df = df[df[k].notnull()]
            return df, [], {'index_label': 'id'}

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.SQL]

