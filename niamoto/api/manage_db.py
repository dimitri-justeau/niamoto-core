# coding: utf-8

"""
Init the niamoto database:

    /!\ Assume that the database, the database user, the database schema and
    /!\ the Postgis extension had been created.

    1- Create the niamoto schema.
    2- Register the data provider types
"""

from niamoto.db.schema_manager import SchemaManager
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


def init_db():
    SchemaManager.upgrade_db('head')
