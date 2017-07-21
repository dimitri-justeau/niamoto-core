# coding: utf-8

import sqlalchemy as sa
import pandas as pd

from niamoto.db import metadata as meta
from niamoto.db.connector import Connector
from niamoto.data_marts.fact_tables.base_fact_table import BaseFactTable
from niamoto.exceptions import FactTableNotRegisteredError


class FactTableManager:
    """
    Fact table manager.
    """

    @classmethod
    def register_fact_table(cls, name, dimensions, measure_columns,
                            publisher_cls=None):
        """
        :param name: The name of the fact table.
        :param dimensions: The dimensions of the fact table. Must be
            BaseDimension subclass instances.
        :param measure_columns: An iterable of sqlalchemy columns
            corresponding to fact measurements.
        :param publisher_cls: The publisher class to use for populating the
            fact table. Must be a subclass of BaseFactTablePublisher.
        :return The registered fact table object.
        """
        fact_table = BaseFactTable(
            name,
            dimensions,
            measure_columns,
            publisher_cls=publisher_cls
        )
        fact_table.create_fact_table()
        return fact_table

    @classmethod
    def get_registered_fact_tables(cls):
        """
        :return: The list of registered fact tables.
        """
        sel = sa.select([meta.fact_table_registry, ])
        with Connector.get_connection() as connection:
            return pd.read_sql(
                sel,
                connection,
                index_col=meta.fact_table_registry.c.id.name
            )

    @classmethod
    def assert_fact_table_is_registered(cls, fact_table_name, connection=None):
        """
        Assert that a fact table is registered.
        Raise DimensionNotRegisteredError otherwise.
        :param fact_table_name: The name of the fact table to check.
        :param connection: If passed, use an existing connection.
        """
        sel = meta.fact_table_registry.select().where(
            meta.fact_table_registry.c.name == fact_table_name
        )
        if connection is not None:
            r = connection.execute(sel).rowcount
        else:
            with Connector.get_connection() as connection:
                r = connection.execute(sel).rowcount
        if r == 0:
            m = "The fact table '{}' is not registered in database."
            raise FactTableNotRegisteredError(m.format(fact_table_name))

    @classmethod
    def get_fact_table(cls, fact_table_name, publisher_cls=None):
        """
        Load a registered fact table.
        :param fact_table_name: The name of the fact table to load.
        :param publisher_cls: The fact table publisher class.
        :return: The loaded fact table.
        """
        cls.assert_fact_table_is_registered(fact_table_name)
        return BaseFactTable.load(
            fact_table_name,
            publisher_cls=publisher_cls
        )

    @classmethod
    def delete_fact_table(cls, fact_table_name):
        """
        Delete a registered fact table.
        :param fact_table_name: The name of the fact table to delete.
        """
        cls.get_fact_table(fact_table_name).drop_fact_table()
