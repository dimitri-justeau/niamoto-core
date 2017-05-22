# coding: utf-8

NIAMOTO_SCHEMA = 'niamoto'
NIAMOTO_RASTER_SCHEMA = 'niamoto_raster'
NIAMOTO_VECTOR_SCHEMA = 'niamoto_vector'

DATABASES = {
    "default": {
        'HOST': 'localhost',
        'PORT': '5432',
        'NAME': 'niamoto',
        'USER': 'niamoto',
        'PASSWORD': 'niamoto',
    },
    'test': {
        'HOST': 'localhost',
        'PORT': '5432',
        'NAME': 'niamoto_test',
        'USER': 'niamoto_test',
        'PASSWORD': 'niamoto_test',
    }
}

DEFAULT_DATABASE = DATABASES['default']
TEST_DATABASE = DATABASES['test']

DEFAULT_POSTGRES_SUPERUSER = 'postgres'
DEFAULT_POSTGRES_SUPERUSER_PASSWORD = 'postgres'
