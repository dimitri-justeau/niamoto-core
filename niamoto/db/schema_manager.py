# coding: utf-8

import os
import subprocess

from alembic import migration
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic import environment

import niamoto
from niamoto.db.connector import Connector
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


class SchemaManager:
    """
    Database manager, based on alembic.
    """

    SCRIPT_LOCATION = os.path.join(
        os.path.dirname(os.path.dirname(niamoto.__file__)),
        'migrations',
    )
    CONFIG = Config()
    CONFIG.set_main_option("script_location", SCRIPT_LOCATION)
    SCRIPT = ScriptDirectory.from_config(CONFIG)
    ENV = environment.EnvironmentContext(
        CONFIG,
        SCRIPT,
    )

    @classmethod
    def get_current_revision(cls):
        LOGGER.debug("Getting the current database revision using alembic...")
        with Connector.get_connection() as connection:
            context = migration.MigrationContext.configure(connection)
            rev = context.get_current_revision()
            LOGGER.debug("Current revision is '{}'".format(rev))
            return rev

    @classmethod
    def get_head_revision(cls):
        LOGGER.debug("Getting head revision using alembic...")
        rev = cls.ENV.get_head_revision()
        LOGGER.debug("Head revision is '{}'".format(rev))
        return rev

    @classmethod
    def upgrade_db(cls, revision):
        LOGGER.debug("Upgrading database to revision '{}'...".format(revision))
        wd = os.path.dirname(os.path.dirname(niamoto.__file__))
        result = subprocess.Popen(
            ["alembic", "upgrade", revision],
            cwd=wd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output = result.communicate()
        LOGGER.debug(output)
        LOGGER.debug("Database is now at revision '{}'".format(revision))

    @classmethod
    def downgrade_db(cls, revision):
        LOGGER.debug(
            "Downgrading database to revision '{}'...".format(revision)
        )
        wd = os.path.dirname(os.path.dirname(niamoto.__file__))
        result = subprocess.Popen(
            ["alembic", "downgrade", revision],
            cwd=wd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output = result.communicate()
        LOGGER.debug(output)
        LOGGER.debug("Database is now at revision '{}'".format(revision))
