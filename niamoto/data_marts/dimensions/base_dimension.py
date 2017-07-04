# coding: utf-8

import io

from sqlalchemy.engine.reflection import Inspector
import sqlalchemy as sa
import pandas as pd

from niamoto.db import metadata as meta
from niamoto.db.connector import Connector
from niamoto.conf import settings
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


class BaseDimension:
    """
    Base class representing a dimension in the dimensional modelling.
    """

    PK_COLUMN_NAME = 'id'
    NS_VALUE = 'NS'

    def __init__(self, name, columns, publisher=None):
        """
        :param name: The name of the dimension. The dimension table will have
            this name.
        :param columns: An iterable of sqlalchemy columns objects.
            The primary column is created automatically so it does not have
            to be in the column list.
        :param publisher: The publisher to use for populating the dimension.
        """
        self.name = name
        self.columns = columns
        self.pk = sa.Column(self.PK_COLUMN_NAME, sa.Integer, primary_key=True)
        self._publisher = publisher
        self._exists = False
        dim_schema = settings.NIAMOTO_DIMENSIONS_SCHEMA
        if "{}.{}".format(dim_schema, name) in meta.metadata.tables:
            self._exists = True
        table_args = [name, meta.metadata, self.pk] + self.columns
        self.table = sa.Table(
            *table_args,
            schema=dim_schema,
            extend_existing=self._exists,
        )

    @property
    def publisher(self):
        return self._publisher

    @publisher.setter
    def publisher(self, value):
        self._publisher = value

    def is_created(self, connection=None):
        """
        :param connection: If not None, use an existing connection.
        :return: True if the dimension exists in database.
        """
        close_after = False
        if connection is None:
            connection = Connector.get_engine().connect()
            close_after = True
        inspector = Inspector.from_engine(connection)
        tables = inspector.get_table_names(
            schema=settings.NIAMOTO_DIMENSIONS_SCHEMA
        )
        if close_after:
            connection.close()
        return self.name in tables

    def create_dimension(self, connection=None):
        """
        Create the dimension in database.
        :param connection: If not None, use an existing connection.
        """
        LOGGER.debug("Creating {}".format(self))
        close_after = False
        if connection is None:
            connection = Connector.get_engine().connect()
            close_after = True
        if self.is_created(connection):
            m = "The dimension {} already exists in database. Creation will " \
                "be skipped."
            LOGGER.warning(m.format(self.name))
            return
        self.table.create(connection)
        if close_after:
            connection.close()
        LOGGER.debug("{} successfully created".format(self))

    def populate(self, dataframe):
        """
        Populates the dimension. Assume that the input dataframe had been
        correctly formatted to fit the dimension columns. All the null values
        are set to 'NS' before populating.
        :param dataframe: The dataframe to populate from.
        """
        LOGGER.debug("Populating {}".format(self))
        cols = [c.name for c in self.columns]
        s = io.StringIO()
        dataframe[cols].fillna(value=self.NS_VALUE).to_csv(s, columns=cols)
        s.seek(0)
        sql_copy = \
            """
            COPY {}.{} ({}) FROM STDIN CSV HEADER DELIMITER ',';
            """.format(
                settings.NIAMOTO_DIMENSIONS_SCHEMA,
                self.name,
                ','.join([self.PK_COLUMN_NAME] + cols)
            )
        raw_connection = Connector.get_engine().raw_connection()
        cur = raw_connection.cursor()
        cur.copy_expert(sql_copy, s)
        cur.close()
        raw_connection.commit()
        raw_connection.close()
        LOGGER.debug("{} successfully populated".format(self))

    def populate_from_publisher(self, *args, **kwargs):
        """
        Populates the dimension using its associated publisher.
        """
        LOGGER.debug("Start populating {} using publisher".format(self))
        data = self.publisher.process(*args, **kwargs)[0]
        self.populate(data)

    def get_values(self):
        """
        :return: A dataframe containing the values stored in database for
            the dimension.
        """
        sql = "SELECT * FROM {}.{};".format(
            settings.NIAMOTO_DIMENSIONS_SCHEMA,
            self.name
        )
        with Connector.get_connection() as connection:
            df = pd.read_sql(sql, connection, index_col=self.PK_COLUMN_NAME)
        return df

    def __repr__(self):
        return "BaseDimension('{}', {})".format(
            self.name,
            self.columns
        )
