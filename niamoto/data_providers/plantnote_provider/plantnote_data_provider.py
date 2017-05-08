# coding: utf-8

from niamoto.conf import settings
from niamoto.data_providers import BaseDataProvider
from niamoto.data_providers.plantnote_provider.plantnote_occurrence_provider \
    import PlantnoteOccurrenceProvider
from niamoto.data_providers.plantnote_provider.plantnote_plot_provider \
    import PlantnotePlotProvider


class PlantnoteDataProvider(BaseDataProvider):
    """
    Pl@ntnote Data Provider.
    Provide data from a Pl@ntnote database. The Pl@ntnote database
    must have previously been converted to a SQLite3 database.
    """

    def __init__(self, name, plantnote_db_path,
                 database=settings.DEFAULT_DATABASE):
        super(PlantnoteDataProvider, self).__init__(
            name,
            database=database
        )
        self.plantnote_db_path = plantnote_db_path
        self._occurrence_provider = PlantnoteOccurrenceProvider(
            self,
            self.plantnote_db_path
        )
        self._plot_provider = PlantnotePlotProvider(
            self,
            self.plantnote_db_path
        )

    @property
    def occurrence_provider(self):
        return self._occurrence_provider

    @property
    def plot_provider(self):
        return self._plot_provider

    @classmethod
    def get_type_name(cls):
        return "PLANTNOTE_DATA_PROVIDER"
