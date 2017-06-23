# coding: utf-8

from rpy2.robjects import conversion
from rpy2.robjects import r
from rpy2.robjects import pandas2ri
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter
from rpy2.rinterface import rternalize, StrSexpVector
from rpy2.robjects import globalenv
import pandas as pd

from niamoto.data_publishers.base_data_publisher import BaseDataPublisher
from niamoto.data_publishers.occurrence_data_publisher import \
    OccurrenceDataPublisher
from niamoto.data_publishers.plot_data_publisher import PlotDataPublisher
from niamoto.data_publishers.plot_occurrence_data_publisher import \
    PlotOccurrenceDataPublisher
from niamoto.data_publishers.taxon_data_publisher import TaxonDataPublisher
from niamoto.data_publishers.raster_data_publisher import RasterDataPublisher


class RDataPublisher(BaseDataPublisher):
    """
    R script data publisher.
    """

    def __init__(self, r_script_path):
        super(RDataPublisher, self).__init__()
        self.r_script_path = r_script_path

    @classmethod
    def get_key(cls):
        raise NotImplementedError()

    @classmethod
    def get_description(cls):
        return "R script."

    def _process(self, *args, **kwargs):
        with localconverter(default_converter + pandas2ri.converter):
            globalenv['get_occurrence_dataframe'] = \
                self.get_occurrence_dataframe
            globalenv['get_plot_dataframe'] = self.get_plot_dataframe
            globalenv['get_plot_occurrence_dataframe'] = \
                self.get_plot_occurrence_dataframe
            globalenv['get_taxon_dataframe'] = self.get_taxon_dataframe
            globalenv['get_raster'] = self.get_raster
            r.source(self.r_script_path)
            process_func = r['process']
            df = pandas2ri.ri2py(process_func())
            if isinstance(df, pd.DataFrame):
                return int32_to_int64(fill_str_empty_with_nan(df)), [], {}
            return df, [], {}

    @staticmethod
    @rternalize
    def get_occurrence_dataframe(properties=None):
        convert = default_converter + pandas2ri.converter
        with conversion.localconverter(convert):
            df = OccurrenceDataPublisher().process(properties=properties)[0]
            return pandas2ri.py2ri(fill_str_nan_with_empty(df))

    @staticmethod
    @rternalize
    def get_plot_dataframe(properties=None):
        convert = default_converter + pandas2ri.converter
        with conversion.localconverter(convert):
            df = PlotDataPublisher().process(properties=properties)[0]
            return pandas2ri.py2ri(fill_str_nan_with_empty(df))

    @staticmethod
    @rternalize
    def get_plot_occurrence_dataframe():
        convert = default_converter + pandas2ri.converter
        with conversion.localconverter(convert):
            df = PlotOccurrenceDataPublisher().process()[0]
            return pandas2ri.py2ri(fill_str_nan_with_empty(df))

    @staticmethod
    @rternalize
    def get_taxon_dataframe(include_mptt=False):
        convert = default_converter + pandas2ri.converter
        with conversion.localconverter(convert):
            df = TaxonDataPublisher().process(include_mptt=include_mptt)[0]
            return pandas2ri.py2ri(fill_str_nan_with_empty(df))

    @staticmethod
    @rternalize
    def get_raster(raster_name):
        convert = default_converter
        with conversion.localconverter(convert):
            raster_str = RasterDataPublisher().process(raster_name[0])[0]
            return StrSexpVector((raster_str, ))

    @classmethod
    def get_publish_formats(cls):
        return [cls.CSV]


def fill_str_nan_with_empty(df):
    #  Fill empty str values with nan
    df.is_copy = False
    if len(df) == 0:
        return df
    for c in df.columns:
        if df[c].dtype == object:
            df[c].fillna(value='', inplace=True)
    return df


def fill_str_empty_with_nan(df):
    df.is_copy = False
    if len(df) == 0:
        return df
    for c in df.columns:
        if df[c].dtype == object:
            df[c].replace(to_replace='', value=pd.np.NaN, inplace=True)
    return df


def int32_to_int64(df):
    df.is_copy = False
    if len(df) == 0:
        return df
    for c in df.columns:
        if df[c].dtype == pd.np.int32:
            df[c] = df[c].astype(pd.np.int64)
    return df
