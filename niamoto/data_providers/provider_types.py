# coding: utf-8

from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider
from niamoto.data_providers.csv_provider import CsvDataProvider


PROVIDER_TYPES = {
    PlantnoteDataProvider.get_type_name(): PlantnoteDataProvider,
    CsvDataProvider.get_type_name(): CsvDataProvider,
}
