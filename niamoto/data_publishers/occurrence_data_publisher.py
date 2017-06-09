# coding: utf-8

from sqlalchemy import select
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

    def _process(self, *args, **kwargs):
        with Connector.get_connection() as connection:
            sel = select([meta.occurrence])
            return pd.read_sql(sel, connection)

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, ]
