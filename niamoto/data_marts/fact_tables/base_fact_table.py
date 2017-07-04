# coding: utf-8

from sqlalchemy.engine.reflection import Inspector
import sqlalchemy as sa

from niamoto.db import metadata as meta
from niamoto.db.connector import Connector
from niamoto.conf import settings
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


class BaseFactTable:
    """
    Base class representing a fact table in the dimensional modelling.
    """

    def __init__(self, name, dimensions, measurement_columns):
        """
        :param name: The name of the fact table.
        :param dimensions: The dimensions of the fact table. Must be
            BaseDimension instances.
        :param measurement_columns: An iterable of sqlalchemy columns
            corresponding to fact measurements.
        """
        self.name = name
        self.dimensions = dimensions
        self.measurement_columns = measurement_columns
        self._exists = False
        fact_schema = settings.NIAMOTO_FACT_TABLES_SCHEMA
        if "{}.{}".format(fact_schema, name) in meta.metadata.tables:
            self._exists = True
        composed_pk = [
            sa.Column(
                "{}_id".format(dim.name),
                sa.ForeignKey("{}.{}.{}".format(
                    dim.table.schema,
                    dim.name,
                    dim.PK_COLUMN_NAME
                )),
                primary_key=True
            ) for dim in self.dimensions
        ]
        table_args = [name, meta.metadata] + composed_pk + measurement_columns
        self.table = sa.Table(
            *table_args,
            schema=fact_schema,
            extend_existing=self._exists
        )

    def is_created(self, connection=None):
        """
        :param connection: If not None, use an existing connection.
        :return: True if the fact table exists in database.
        """
        close_after = False
        if connection is None:
            connection = Connector.get_engine().connect()
            close_after = True
        inspector = Inspector.from_engine(connection)
        tables = inspector.get_table_names(
            schema=settings.NIAMOTO_FACT_TABLES_SCHEMA
        )
        if close_after:
            connection.close()
        return self.name in tables

    def create_fact_table(self, connection=None):
        """
        Create the fact table in database.
        :param connection: If not None, use an existing connection.
        """
        close_after = False
        if connection is None:
            connection = Connector.get_engine().connect()
            close_after = True
        if self.is_created(connection):
            m = "The fact table {} already exists in database. Creation " \
                "will be skipped."
            LOGGER.warning(m.format(self.name))
            return
        self.table.create(connection)
        if close_after:
            connection.close()

    def __repr__(self):
        return "BaseFactTable('{}', {}, {})".format(
            self.name,
            self.dimensions,
            self.measurement_columns,
        )
