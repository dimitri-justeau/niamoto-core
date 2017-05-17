# coding: utf-8

from geoalchemy2.shape import from_shape
from shapely.geometry import Point


def get_occurrence_data_1(data_provider):
    occ_1 = [
        {
            'id': 0,
            'provider_id': data_provider.db_id,
            'provider_pk': 0,
            'location': from_shape(Point(166.5521, -22.0939), srid=4326),
            'properties': {},
        },
        {
            'id': 1,
            'provider_id': data_provider.db_id,
            'provider_pk': 1,
            'location': from_shape(Point(166.551, -22.098), srid=4326),
            'properties': {},
        },
        {
            'id': 2,
            'provider_id': data_provider.db_id,
            'provider_pk': 2,
            'location': from_shape(Point(166.552, -22.097), srid=4326),
            'properties': {},
        },
        {
            'id': 3,
            'provider_id': data_provider.db_id,
            'provider_pk': 5,
            'location': from_shape(Point(166.553, -22.099), srid=4326),
            'properties': {},
        },
    ]
    return occ_1


def get_occurrence_data_2(data_provider):
    occ_2 = [
        {
            'id': 4,
            'provider_id': data_provider.db_id,
            'provider_pk': 0,
            'location': from_shape(Point(166.5511, -22.09739), srid=4326),
            'properties': {},
        },
    ]
    return occ_2


def get_plot_data_1(data_provider):
    plot_1 = [
        {
            'id': 0,
            'provider_id': data_provider.db_id,
            'provider_pk': 0,
            'name': 'plot_1_1',
            'location': from_shape(Point(166.5521, -22.0939), srid=4326),
            'properties': {},
        },
        {
            'id': 1,
            'provider_id': data_provider.db_id,
            'provider_pk': 1,
            'name': 'plot_1_2',
            'location': from_shape(Point(166.551, -22.098), srid=4326),
            'properties': {},
        },
        {
            'id': 2,
            'provider_id': data_provider.db_id,
            'provider_pk': 2,
            'name': 'plot_1_3',
            'location': from_shape(Point(166.552, -22.097), srid=4326),
            'properties': {},
        },
        {
            'id': 3,
            'provider_id': data_provider.db_id,
            'provider_pk': 5,
            'name': 'plot_1_4',
            'location': from_shape(Point(166.553, -22.099), srid=4326),
            'properties': {},
        },
    ]
    return plot_1


def get_plot_data_2(data_provider):
    plot_2 = [
        {
            'id': 4,
            'provider_id': data_provider.db_id,
            'provider_pk': 0,
            'name': 'plot_2_1',
            'location': from_shape(Point(166.5511, -22.09739), srid=4326),
            'properties': {},
        },
        {
            'id': 5,
            'provider_id': data_provider.db_id,
            'provider_pk': 1,
            'name': 'plot_2_2',
            'location': from_shape(Point(166.5511, -22.09749), srid=4326),
            'properties': {},
        },
    ]
    return plot_2


def get_plot_occurrence_data_1(data_provider):
    plot_occ_1 = [
        {
            'plot_id': 0,
            'occurrence_id': 0,
            'provider_id': data_provider.db_id,
            'provider_plot_pk': 0,
            'provider_occurrence_pk': 0,
            'occurrence_identifier': 'PLOT1_000',
        },
        {
            'plot_id': 1,
            'occurrence_id': 0,
            'provider_id': data_provider.db_id,
            'provider_plot_pk': 1,
            'provider_occurrence_pk': 0,
            'occurrence_identifier': 'PLOT2_000',
        },
        {
            'plot_id': 1,
            'occurrence_id': 1,
            'provider_id': data_provider.db_id,
            'provider_plot_pk': 1,
            'provider_occurrence_pk': 1,
            'occurrence_identifier': 'PLOT2_001',
        },
        {
            'plot_id': 1,
            'occurrence_id': 2,
            'provider_id': data_provider.db_id,
            'provider_plot_pk': 1,
            'provider_occurrence_pk': 2,
            'occurrence_identifier': 'PLOT2_002',
        },
        {
            'plot_id': 2,
            'occurrence_id': 3,
            'provider_id': data_provider.db_id,
            'provider_plot_pk': 2,
            'provider_occurrence_pk': 5,
            'occurrence_identifier': 'PLOT2_002',
        },
    ]
    return plot_occ_1


def get_plot_occurrence_data_2(data_provider):
    plot_occ_2 = [
        {
            'plot_id': 4,
            'occurrence_id': 4,
            'provider_id': data_provider.db_id,
            'provider_plot_pk': 0,
            'provider_occurrence_pk': 0,
            'occurrence_identifier': 'PLOT5_004',
        },
    ]
    return plot_occ_2
