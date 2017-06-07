# coding: utf-8

from os.path import exists, isfile

from niamoto.data_providers.base_data_provider import BaseDataProvider
from niamoto.data_providers.plantnote_provider.plantnote_occurrence_provider \
    import PlantnoteOccurrenceProvider
from niamoto.data_providers.plantnote_provider.plantnote_plot_provider \
    import PlantnotePlotProvider
from niamoto.data_providers.plantnote_provider \
    .plantnote_plot_occurrence_provider import PlantnotePlotOccurrenceProvider
from niamoto.exceptions import DataSourceNotFoundError


class PlantnoteDataProvider(BaseDataProvider):
    """
    Pl@ntnote Data Provider.
    Provide data from a Pl@ntnote database. The Pl@ntnote database
    must have previously been converted to a SQLite3 database.
    """

    def __init__(self, name, plantnote_db_path=None):
        super(PlantnoteDataProvider, self).__init__(name)
        self.plantnote_db_path = plantnote_db_path
        self._occurrence_provider = PlantnoteOccurrenceProvider(
            self,
            self.plantnote_db_path
        )
        self._plot_provider = PlantnotePlotProvider(
            self,
            self.plantnote_db_path
        )
        self._plot_occurrence_provider = PlantnotePlotOccurrenceProvider(
            self,
            self.plantnote_db_path
        )

    def sync(self, insert=True, update=True, delete=True,
             sync_occurrence=True, sync_plot=True,
             sync_plot_occurrence=True):
        db_path = self.plantnote_db_path
        if not exists(db_path) or not isfile(db_path):
            m = "The Pl@ntnote database '{}' does not exist.".format(
                db_path
            )
            raise DataSourceNotFoundError(m)
        return super(PlantnoteDataProvider, self).sync(
            insert=True,
            update=True,
            delete=True,
            sync_occurrence=True,
            sync_plot=True,
            sync_plot_occurrence=True
        )

    @property
    def occurrence_provider(self):
        return self._occurrence_provider

    @property
    def plot_provider(self):
        return self._plot_provider

    @property
    def plot_occurrence_provider(self):
        return self._plot_occurrence_provider

    @classmethod
    def get_type_name(cls):
        return "PLANTNOTE"
