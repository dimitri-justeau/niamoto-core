# coding: utf-8

import sqlalchemy as sa
import pandas as pd

from niamoto.data_providers.base_plot_occurrence_provider \
    import BasePlotOccurrenceProvider
from niamoto.exceptions import MalformedDataSourceError


class SQLPlotOccurrenceProvider(BasePlotOccurrenceProvider):
    """
    SQL plot-occurrence provider. Instantiated with a sql query, that must
    return AT LEAST the following columns:
        plot_id -> The provider's id for the plot.
        occurrence_id -> The provider's id for the occurrence.
        occurrence_identifier -> The occurrence identifier in the plot.
    All the remaining column will be ignored.
    """

    REQUIRED_COLUMNS = set([
        'plot_id',
        'occurrence_id',
        'occurrence_identifier',
    ])

    def __init__(self, data_provider, plot_occurrence_sql):
        super(SQLPlotOccurrenceProvider, self).__init__(data_provider)
        self.plot_occurrence_sql = plot_occurrence_sql

    def get_provider_plot_occurrence_dataframe(self):
        connection = sa.create_engine(self.data_provider.db_url).connect()
        df = pd.read_sql(
            self.plot_occurrence_sql,
            connection,
            index_col=['plot_id', 'occurrence_id']
        )
        cols = set(list(df.columns) + ['plot_id', 'occurrence_id'])
        inter = cols.intersection(self.REQUIRED_COLUMNS)
        if not inter == self.REQUIRED_COLUMNS:
            m = "The queried data does not contains the required columns " \
                "('plot_id', 'occurrence_id', 'occurrence_identifier')" \
                ", queried data has: {}".format(cols)
            raise MalformedDataSourceError(m)
        if len(df) == 0:
            return df
        property_cols = cols.difference(self.REQUIRED_COLUMNS)
        df.drop(property_cols, axis=1, inplace=True)
        return df

