# coding: utf-8

import io

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

    def __init__(self, name, dimensions, measurement_columns,
                 publisher_cls=None):
        """
        :param name: The name of the fact table.
        :param dimensions: The dimensions of the fact table. Must be
            BaseDimension subclass instances.
        :param measurement_columns: An iterable of sqlalchemy columns
            corresponding to fact measurements.
        :param publisher_cls: The publisher class to use for populating the
            dimension. Must be a subclass of BaseFactTablePublisher.
        """
        self.name = name
        self.dimensions = dimensions
        self._dimensions_dict = {d.name: d for d in self.dimensions}
        self.measurement_columns = measurement_columns
        self._publisher_cls = publisher_cls
        self._publisher = None
        if self._publisher_cls is not None:
            self._publisher = self._publisher_cls(self)
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
        self.columns = composed_pk + measurement_columns
        table_args = [name, meta.metadata] + self.columns
        self.table = sa.Table(
            *table_args,
            schema=fact_schema,
            extend_existing=self._exists
        )

    @property
    def publisher_cls(self):
        return self._publisher_cls

    @publisher_cls.setter
    def publisher_cls(self, value):
        self._publisher_cls = value
        if self._publisher_cls is not None:
            self._publisher = self._publisher_cls(self)

    @property
    def publisher(self):
        return self._publisher

    def get_dimension(self, dimension_name):
        return self._dimensions_dict[dimension_name]

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

    def drop_fact_table(self, connection=None):
        """
        Drop an existing fact table.
        :param connection: If not None, use an existing connection.
        """
        LOGGER.debug("Dropping {}".format(self))
        close_after = False
        if connection is None:
            connection = Connector.get_engine().connect()
            close_after = True
        if not self.is_created(connection):
            m = "The fact table {} does not exists in database. Drop will " \
                "be skipped"
            LOGGER.warning(m.format(self.name))
            return
        with connection.begin():
            self.table.drop(connection)
            # delete = meta.dimension_registry.delete().where(
            #     meta.dimension_registry.c.name == self.name
            # )
            # connection.execute(delete) TODO FACT TABLE REGISTRY
        if close_after:
            connection.close()
        LOGGER.debug("{} successfully dropped".format(self))

    def populate(self, dataframe):
        """
        Populates the fact table. Assume that the input dataframe had been
        correctly formatted to fit the fact table columns. All the null
        measure are set to 0 before populating.
        :param dataframe: The dataframe to populate from.
        """
        LOGGER.debug("Populating {}".format(self))
        cols = [c.name for c in self.columns]
        s = io.StringIO()
        dataframe[cols].fillna(value=0).to_csv(s, columns=cols, index=False)
        s.seek(0)
        sql_copy = \
            """
            COPY {}.{} ({}) FROM STDIN CSV HEADER DELIMITER ',';
            """.format(
                settings.NIAMOTO_FACT_TABLES_SCHEMA,
                self.name,
                ','.join(cols)
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
        Populates the fact table using its associated publisher.
        """
        LOGGER.debug("Start populating {} using publisher".format(self))
        data = self.publisher.process(*args, **kwargs)[0]
        self.populate(data)

    def __repr__(self):
        return "BaseFactTable('{}', {}, {})".format(
            self.name,
            self.dimensions,
            self.measurement_columns,
        )
