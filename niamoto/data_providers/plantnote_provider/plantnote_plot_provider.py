# coding: utf-8

from sqlalchemy import create_engine, select
import pandas as pd

from niamoto.data_providers import BasePlotProvider


class PlantnotePlotProvider(BasePlotProvider):
    """
    Pl@ntnote Plot Provider.
    Provide plots from a Pl@ntnote database. The Pl@ntnote database
    must have previously been converted to a SQLite3 database.
    """

    def __init__(self, data_provider, plantnote_db_path):
        super(PlantnotePlotProvider, self).__init__(data_provider)
        self.plantnote_db_path = plantnote_db_path

    def get_provider_plot_dataframe(self):
        engine = create_engine('sqlite:///{}'.format(self.plantnote_db_path))
