# coding: utf-8

NIAMOTO_SCHEMA = 'niamoto'
NIAMOTO_RASTER_SCHEMA = 'niamoto_raster'
NIAMOTO_VECTOR_SCHEMA = 'niamoto_vector'
NIAMOTO_DIMENSIONS_SCHEMA = 'niamoto_dimensions'
NIAMOTO_FACT_TABLES_SCHEMA = 'niamoto_fact_tables'
NIAMOTO_SSDM_SCHEMA = 'niamoto_ssdm'

DATABASES = {
    "niamoto": {
        'HOST': 'localhost',
        'PORT': '5432',
        'NAME': 'niamoto',
        'USER': 'niamoto',
        'PASSWORD': 'niamoto',
    },
}

NIAMOTO_DATABASE = DATABASES['niamoto']

DEFAULT_POSTGRES_SUPERUSER = 'postgres'
DEFAULT_POSTGRES_SUPERUSER_PASSWORD = 'postgres'
