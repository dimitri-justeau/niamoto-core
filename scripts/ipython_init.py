# coding: utf-8

from IPython import get_ipython
ipython = get_ipython()

from niamoto.conf import set_settings

set_settings()

from niamoto.data_publishers.r_data_publisher import RDataPublisher, \
    globalenv

globalenv['get_occurrence_dataframe'] = RDataPublisher.get_occurrence_dataframe

ipython.magic("load_ext rpy2.ipython")

