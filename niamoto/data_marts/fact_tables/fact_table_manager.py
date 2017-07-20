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
    def delete_fact_table(cls, fact_table_name):
        """
        Delete a registered fact table.
        :param fact_table_name: The name of the fact table to delete.
        """
        # TODO USE FACT TABLE REGISTRY
        ft = BaseFactTable(fact_table_name, [], [])
        ft.drop_fact_table()
