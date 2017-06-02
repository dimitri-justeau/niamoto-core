# coding: utf-8

NIAMOTO_SCHEMA = 'niamoto'
NIAMOTO_RASTER_SCHEMA = 'niamoto_raster'
NIAMOTO_VECTOR_SCHEMA = 'niamoto_vector'

DATABASES = {
    'niamoto': {
        'HOST': 'localhost',
        'PORT': '5432',
        'NAME': 'niamoto_test',
        'USER': 'niamoto_test',
        'PASSWORD': 'niamoto_test',
    }
}

NIAMOTO_DATABASE = DATABASES['niamoto']
TEST_DATABASE = NIAMOTO_DATABASE

DEFAULT_POSTGRES_SUPERUSER = 'postgres'
DEFAULT_POSTGRES_SUPERUSER_PASSWORD = 'postgres'
