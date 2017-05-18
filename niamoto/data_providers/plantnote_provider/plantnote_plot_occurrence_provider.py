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
            plot_occ_table = metadata.tables['Inventaires']
            sel = select([
                plot_occ_table.c["ID Parcelle"].label("provider_plot_pk"),
                plot_occ_table.c["ID Individus"].label(
                    "provider_occurrence_pk"
                ),
                plot_occ_table.c["Identifiant"].label(
                    "occurrence_identifier"
                ),
            ])
            df = pd.read_sql(
                sel,
                connection,
                index_col=["provider_plot_pk", "provider_occurrence_pk"]
            )
            return df
        except:
            raise
        finally:
            if connection:
                connection.close()
                eng.dispose()

