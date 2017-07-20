# coding: utf-8

from niamoto.data_marts.fact_tables.base_fact_table import BaseFactTable


class FactTableManager:
    """
    Fact table manager.
    """

    @classmethod
    def delete_fact_table(cls, fact_table_name):
        """
        Delete a registered fact table.
        :param fact_table_name: The name of the fact table to delete.
        """
        # TODO USE FACT TABLE REGISTRY
        ft = BaseFactTable(fact_table_name, [], [])
        ft.drop_fact_table()
