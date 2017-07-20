# coding: utf-8

import sqlalchemy as sa
import pandas as pd

from niamoto.data_marts.dimensions.base_dimension import BaseDimension
from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.data_publishers.base_fact_table_publisher import \
    BaseFactTablePublisher


class TestPublisher(BaseDataPublisher):
    def _process(self, *args, **kwargs):
        df = pd.DataFrame([
            {'idx': 0, 'value': 1, 'category': 'cat1'},
            {'idx': 1, 'value': 2, 'category': 'cat2'},
            {'idx': 2, 'value': 3, 'category': 'cat2'},
            {'idx': 3, 'value': 4, 'category': 'cat1'},
            {'idx': 4, 'value': 5, 'category': None},
        ])
        df.set_index(['idx'], inplace=True)
        return df


class TestDimension(BaseDimension):

    def __init__(self, name="test_dimension", publisher=TestPublisher()):
        super(TestDimension, self).__init__(
            name,
            [sa.Column('value', sa.Integer), sa.Column('category', sa.String)],
            publisher=publisher
        )

    @classmethod
    def load(cls, dimension_name, label_col=None):
        return cls()

    @classmethod
    def get_description(cls):
        return 'Test dimension'

    @classmethod
    def get_key(cls):
        return 'TEST_DIMENSION'


class TestFactTablePublisher(BaseFactTablePublisher):

    def _process(self, *args, **kwargs):
        df = pd.DataFrame([
            {'dim_1_id': 0, 'dim_2_id': 1, 'measure_1': 1},
            {'dim_1_id': 0, 'dim_2_id': 2, 'measure_1': 2},
            {'dim_1_id': 0, 'dim_2_id': 3, 'measure_1': 3},
            {'dim_1_id': 0, 'dim_2_id': 4, 'measure_1': 4},
            {'dim_1_id': 1, 'dim_2_id': 2, 'measure_1': 3},
            {'dim_1_id': 2, 'dim_2_id': 4, 'measure_1': 2},
            {'dim_1_id': 3, 'dim_2_id': 0, 'measure_1': 1},
            {'dim_1_id': 3, 'dim_2_id': 4, 'measure_1': None},
            {'dim_1_id': 3, 'dim_2_id': 3, 'measure_1': 5},
            {'dim_1_id': 3, 'dim_2_id': 2, 'measure_1': 4},
            {'dim_1_id': 3, 'dim_2_id': 1, 'measure_1': 6},
        ])
        return df

    @classmethod
    def get_key(cls):
        return 'TEST_FACT_TABLE_PUBLISHER'
