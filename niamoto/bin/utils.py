# coding: utf-8


def format_datetime_to_date(obj):
    return obj.strftime("%Y/%m/%d")


def format_datetime_to_date_hour(obj):
    return obj.strftime("%Y/%m/%d %H:%M:%S")
