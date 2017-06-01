# coding: utf-8

"""
Init the niamoto database:

    /!\ Assume that the database, the database user, the database schema and
    /!\ the Postgis extension had been created.

    1- Create the niamoto schema.
    2- Populate the taxonomic referential.
    3- Register the data provider types
"""

import os
import subprocess

import niamoto
from niamoto.conf import settings
from niamoto.db.connector import Connector
from niamoto.db.creator import Creator
from niamoto.taxonomy.populate import populate_ncpippn_taxon_database, \
    load_ncpippn_taxon_dataframe_from_json
from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


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


def upgrade_db_to_head():
    wd = os.path.dirname(os.path.dirname(niamoto.__file__))
    subprocess.call(
        ["alembic", "upgrade", 'head'],
        cwd=wd,
        stdout=LOGGER.debug
    )
