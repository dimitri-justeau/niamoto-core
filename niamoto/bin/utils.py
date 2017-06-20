# coding: utf-8

import pandas as pd


def format_datetime_to_date(obj):
    if pd.isnull(obj):
        return ''
    return obj.strftime("%Y/%m/%d")
