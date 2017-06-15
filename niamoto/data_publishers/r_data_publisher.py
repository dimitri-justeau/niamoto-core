# coding: utf-8

from rpy2.robjects import conversion
from rpy2.robjects import r
from rpy2.robjects import pandas2ri
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter
from rpy2.rinterface import rternalize
from rpy2.robjects import globalenv
import pandas as pd

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.data_publishers.occurrence_data_publisher import \
    OccurrenceDataPublisher


class RDataPublisher(BaseDataPublisher):
    """
    R script data publisher.
    """

    def __init__(self, r_script_path):
        super(RDataPublisher, self).__init__()
        self.r_script_path = r_script_path

    @classmethod
    def get_key(cls):
        return 'R'

    def _process(self, *args, **kwargs):
        with localconverter(default_converter + pandas2ri.converter):
            globalenv['get_occurrence_dataframe'] = \
                self.get_occurrence_dataframe
            r.source(self.r_script_path)
            process_func = r['process']
            df = pandas2ri.ri2py(process_func())
            return int32_to_int64(fill_str_empty_with_nan(df))

    @staticmethod
    @rternalize
    def get_occurrence_dataframe(properties=None):
        convert = default_converter + pandas2ri.converter
        with conversion.localconverter(convert):
            df = OccurrenceDataPublisher().process(properties=properties)[0]
            return pandas2ri.py2ri(fill_str_nan_with_empty(df[:10]))

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV, cls.STREAM]


def fill_str_nan_with_empty(df):
    #  Fill empty str values with nan
    df.is_copy = False
    for c in df.columns:
        if df[c].dtype == object:
            df[c].fillna(value='', inplace=True)
    return df


def fill_str_empty_with_nan(df):
    df.is_copy = False
    for c in df.columns:
        if df[c].dtype == object:
            df[c].replace(to_replace='', value=pd.np.NaN, inplace=True)
    return df


def int32_to_int64(df):
    df.is_copy = False
    for c in df.columns:
        if df[c].dtype == pd.np.int32:
            df[c] = df[c].astype(pd.np.int64)
    return df
