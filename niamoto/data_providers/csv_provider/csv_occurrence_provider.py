# coding: utf-8

from os.path import exists, isfile

import pandas as pd

from niamoto.data_providers import BaseOccurrenceProvider
from niamoto.exceptions import DataSourceNotFoundError, \
    MalformedDataSourceError


class CsvOccurrenceProvider(BaseOccurrenceProvider):
    """
    Csv occurrence provider.
    The csv file must contain AT LEAST the following columns:
        id -> The provider's identifier for the occurrence.
        taxon_id -> The provider's taxon id for the occurrence.
        x -> The longitude of the occurrence (WGS84).
        y -> The latitude of the occurrence (WGS84).
    All the remaining column will be stored as properties.
    """

    REQUIRED_COLUMNS = set(['id', 'taxon_id', 'x', 'y'])

    def __init__(self, data_provider, occurrence_csv_path):
        super(CsvOccurrenceProvider, self).__init__(data_provider)
        if not exists(occurrence_csv_path) or not isfile(occurrence_csv_path):
            m = "The occurrence csv file '{}' does not exist.".format(
                occurrence_csv_path
            )
            raise DataSourceNotFoundError(m)
        self.occurrence_csv_path = occurrence_csv_path

    def get_provider_occurrence_dataframe(self):
        try:
            df = pd.read_csv(self.occurrence_csv_path, index_col='id')
        except ValueError:
            m = "The csv file is not valid, it must contains the following " \
                "columns: ('plot_id', 'occurrence_id', " \
                "'occurrence_identifier')"
            raise MalformedDataSourceError(m)
        cols = set(list(df.columns) + ['id', ])
        inter = cols.intersection(self.REQUIRED_COLUMNS)
        if not inter == self.REQUIRED_COLUMNS:
            m = "The csv file does not contains the required columns " \
                "('id', 'taxon_id', 'x', 'y'), csv has: {}".format(cols)
            raise MalformedDataSourceError(m)
        if len(df) == 0:
            return df
        property_cols = cols.difference(self.REQUIRED_COLUMNS)
        if len(property_cols) > 0:
            properties = df[list(property_cols)].apply(
                lambda x: x.to_json(),
                axis=1
            )
        else:
            properties = '{}'
        df.drop(property_cols, axis=1, inplace=True)
        df['properties'] = properties
        location = df[['x', 'y']].apply(
            lambda x: "SRID=4326;POINT({} {})".format(x['x'], x['y']),
            axis=1
        )
        df['location'] = location
        df.drop(['x', 'y'], axis=1, inplace=True)
        return df
