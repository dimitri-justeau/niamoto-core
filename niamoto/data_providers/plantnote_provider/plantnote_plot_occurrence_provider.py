# coding: utf-8

from sqlalchemy import *
import pandas as pd

from niamoto.data_providers import BasePlotOccurrenceProvider


class PlantnotePlotOccurrenceProvider(BasePlotOccurrenceProvider):
    """
    Pl@ntnote Plot-Occurrence provider.
    """

    def __init__(self, data_provider, plantnote_db_path):
        super(PlantnotePlotOccurrenceProvider, self).__init__(data_provider)
        self.plantnote_db_path = plantnote_db_path

    def get_provider_plot_occurrence_dataframe(self):
        db_str = 'sqlite:///{}'.format(self.plantnote_db_path)
        eng = create_engine(db_str)
        connection = eng.connect()
        try:
            metadata = MetaData()
            metadata.reflect(eng)
            #  Needed tables
            plot_occ_table = metadata.tables['Inventaires']

            return df
        except:
            raise
        finally:
            if connection:
                connection.close()
                eng.dispose()

