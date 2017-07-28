# coding: utf-8

import io
from datetime import datetime

from sqlalchemy.engine.reflection import Inspector
import sqlalchemy as sa
import pandas as pd

from niamoto.db import metadata as meta
from niamoto.db.connector import Connector
from niamoto.conf import settings
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


DIMENSION_TYPE_REGISTRY = {}


class DimensionMeta(type):

    def __init__(cls, *args, **kwargs):
        try:
            DIMENSION_TYPE_REGISTRY[cls.get_key()] = {
                'class': cls,
                'description': cls.get_description()
            }
        except NotImplementedError:
            pass
        return super(DimensionMeta, cls).__init__(cls)


class BaseDimension(metaclass=DimensionMeta):
    """
    Base class representing a dimension in the dimensional modelling.
    """

    PK_COLUMN_NAME = 'id'
    NS_VALUES = {
        sa.VARCHAR: 'NS',
        sa.NUMERIC: pd.np.nan,
        sa.Text: 'NS',
        sa.String: 'NS',
    }
    DEFAULT_NS_VALUE = pd.np.nan

    def __init__(self, name, columns, publisher=None, label_col='label',
                 properties={}):
        """
        :param name: The name of the dimension. The dimension table will have
            this name.
        :param: label_col: The name of the label columns. Default is 'label'.
        :param columns: An iterable of sqlalchemy columns objects.
            The primary key column is created automatically so it does not
            have to be in the column list.
        :param publisher: The publisher to use for populating the dimension.
        :param properties: A dict of arbitrary properties.
        """
        self.name = name
        self.columns = columns
        self.label_col = label_col
        self.pk = sa.Column(self.PK_COLUMN_NAME, sa.Integer, primary_key=True)
        self._publisher = publisher
        self._exists = False
        self.properties = properties
        dim_schema = settings.NIAMOTO_DIMENSIONS_SCHEMA
        if "{}.{}".format(dim_schema, name) in meta.metadata.tables:
            self._exists = True
        table_args = [name, meta.metadata, self.pk] + self.columns
        self.table = sa.Table(
            *table_args,
            schema=dim_schema,
            extend_existing=self._exists
        )

    @property
    def publisher(self):
        return self._publisher

    @publisher.setter
    def publisher(self, value):
        self._publisher = value

    @classmethod
    def get_key(cls):
        """
        :return: The key of the conformed dimension.
        """
        raise NotImplementedError()

    @classmethod
    def get_description(cls):
        """
        :return: The description of the conformed dimension.
        """
        raise NotImplementedError()

    @classmethod
    def load(cls, dimension_name, label_col='label'):
        """
        Load a Dimension instance from its name.
        :param dimension_name: The name of the dimension.
        :param label_col: The label column name of the dimension.
        :return: The loaded dimension
        """
        return cls(name=dimension_name, label_col=label_col)

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
        with connection.begin():
            self.table.create(connection)
            ins = meta.dimension_registry.insert().values({
                'name': self.name,
                'dimension_type_key': self.get_key(),
                'label_column': self.label_col,
                'date_create': datetime.now(),
                'properties': self.properties,
            })
            connection.execute(ins)
        if close_after:
            connection.close()
        LOGGER.debug("{} successfully created".format(self))

    def drop_dimension(self, connection=None):
        """
        Drop an existing dimension.
        :param connection: If not None, use an existing connection.
        """
        LOGGER.debug("Dropping {}".format(self))
        close_after = False
        if connection is None:
            connection = Connector.get_engine().connect()
            close_after = True
        if not self.is_created(connection):
            m = "The dimension {} does not exists in database. Drop will " \
                "be skipped"
            LOGGER.warning(m.format(self.name))
            return
        with connection.begin():
            self.table.drop(connection)
            delete = meta.dimension_registry.delete().where(
                meta.dimension_registry.c.name == self.name
            )
            connection.execute(delete)
        if close_after:
            connection.close()
        LOGGER.debug("{} successfully dropped".format(self))

    def populate(self, dataframe, append_ns_row=True):
        """
        Populates the dimension. Assume that the input dataframe had been
        correctly formatted to fit the dimension columns. All the null values
        are set to the corresponding type NS before populating.
        :param dataframe: The dataframe to populate from.
        :param append_ns_row: If True, append a NS row to the dimension.
        """
        LOGGER.debug("Populating {}".format(self))
        cols = [c.name for c in self.columns]
        s = io.StringIO()
        ns = {}
        for c in self.columns:
            if type(c.type) in self.NS_VALUES:
                ns[c.name] = self.NS_VALUES[type(c.type)]
            else:
                ns[c.name] = self.DEFAULT_NS_VALUE
        dataframe[cols].fillna(value=ns).to_csv(s, columns=cols)
        if append_ns_row:
            idx = 0
            if len(dataframe.index) > 0:
                idx = dataframe.index.max() + 1
            ns_row = pd.DataFrame(ns, index=[idx])
            ns_row[cols].to_csv(s, columns=cols, header=False)
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

    def populate_from_publisher(self, *args, append_ns_row=True, **kwargs):
        """
        Populates the dimension using its associated publisher.
        :param append_ns_row: If True, append a NS row to the dimension.
        """
        LOGGER.debug("Start populating {} using publisher".format(self))
        data = self.publisher.process(*args, **kwargs)[0]
        self.populate(data, append_ns_row=append_ns_row)

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

    def get_labels(self):
        return self.get_values()[self.label_col]

    def __repr__(self):
        return "{}('{}', {})".format(
            self.__class__.__name__,
            self.name,
            self.columns
        )
