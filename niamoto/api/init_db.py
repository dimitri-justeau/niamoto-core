# coding: utf-8

"""
Init the niamoto database:

    /!\ Assume that the database, the database user, the database schema and
    /!\ the Postgis extension had been created.

    1- Create the niamoto schema.
    2- Populate the taxonomic referential.
    3- Register the data provider types
"""

from niamoto.conf import settings
from niamoto.db.connector import Connector
from niamoto.db.creator import Creator
from niamoto.taxonomy.populate import populate_ncpippn_taxon_database, \
    load_ncpippn_taxon_dataframe_from_json
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider


def init_db(database=settings.DEFAULT_DATABASE):
    #  1- Create schema
    Creator.create_niamoto_schema(Connector.get_engine(database=database))
    #  2- Populate taxa
    populate_ncpippn_taxon_database(
        load_ncpippn_taxon_dataframe_from_json(),
        database=database
    )
    #  3- Register data provider types
    PlantnoteDataProvider.register_data_provider_type(database=database)
