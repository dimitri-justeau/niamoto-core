# coding: utf-8

from sqlalchemy import select, func
import pandas as pd

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.db import metadata as meta
from niamoto.db.connector import Connector


class PlotOccurrenceDataPublisher(BaseDataPublisher):
    """
    Publish plot/occurrence dataframe.
    """

    @classmethod
    def get_key(cls):
        return 'plots_occurrences'

    @classmethod
    def get_description(cls):
        return "Retrieve the plots/occurrences dataframe."

    def _process(self, *args, properties=None, **kwargs):
        """
        :param properties: List of properties to retain. Can be a python list
            or a comma (',') separated string.
        """
        with Connector.get_connection() as connection:
            sel = select([
                meta.plot_occurrence.c.plot_id.label('plot_id'),
                meta.plot_occurrence.c.occurrence_id.label('occurrence_id'),
                meta.plot_occurrence.c.occurrence_identifier.label(
                    'occurrence_identifier'
                ),
            ])
            df = pd.read_sql(
                sel,
                connection,
                index_col=['plot_id', 'occurrence_id']
            )
            #  Replace None values with nan
            df.fillna(value=pd.np.NAN, inplace=True)
            return df, [], {'index_label': ('plot_id', 'occurrence_id')}

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.STREAM]

