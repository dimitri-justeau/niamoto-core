# coding: utf-8

import platform

if platform.python_version_tuple()[0] != '3':
    raise SystemError("Niamoto is only compatible with Python 3.")

import numpy
from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import to_shape
from psycopg2.extensions import register_adapter, AsIs, adapt

from niamoto import conf

conf.set_niamoto_home()
conf.set_settings()


__version__ = "0.1"


# Teach psycopg2 how to handle certain datatypes.

def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def adapt_geoalchemy2_wkbe_element(element):
    ewkt = "SRID={};{}".format(element.srid, to_shape(element).wkt)
    return AsIs(adapt(ewkt))


register_adapter(numpy.float64, adapt_numpy_float64)
register_adapter(numpy.int64, adapt_numpy_int64)
register_adapter(WKBElement, adapt_geoalchemy2_wkbe_element)
