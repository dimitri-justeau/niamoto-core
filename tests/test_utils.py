# coding: utf-8

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class TestDatabaseManager:
    """
    Object managing the creation, cleaning and destruction of a test
    database.
    """

    POSTGRES_SUPERUSER = 'postgres'
    POSTGRES_SUPERUSER_PASSWORD = 'postgres'
    HOST = 'localhost'

    TEST_USER = 'niamoto_test'
    TEST_PASSWORD = 'niamoto_test'
    TEST_DATABASE = 'niamoto_test'

    @classmethod
    def prompt_superuser(cls):
        _superuser = input(
            "Enter the postgres superuser name (default: '{}'):".format(
                cls.POSTGRES_SUPERUSER,
            )
        )
        if _superuser is not '':
            cls.POSTGRES_SUPERUSER = _superuser

    @classmethod
    def prompt_superuser_password(cls):
        _superuser_password = input(
            "Enter the postgres superuser password (default '{}'):".format(
                cls.POSTGRES_SUPERUSER_PASSWORD,
            )
        )
        if _superuser_password is not '':
            cls.POSTGRES_SUPERUSER_PASSWORD = _superuser_password

    @classmethod
    def prompt_host(cls):
        _host = input(
            "Enter the postgres server host (default: '{}'):".format(
                cls.HOST,
            )
        )
        if _host is not '':
            cls.HOST = _host

    @classmethod
    def _get_superuser_connection(cls):
        connection = psycopg2.connect(
            "dbname='{}' user='{}' host='{}' password='{}'".format(
                'postgres',
                cls.POSTGRES_SUPERUSER,
                cls.HOST,
                cls.POSTGRES_SUPERUSER_PASSWORD
            )
        )
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

    @classmethod
    def _get_test_database_connection(cls):
        connection = psycopg2.connect(
            "dbname='{}' user='{}' host='{}' password='{}'".format(
                cls.TEST_DATABASE,
                cls.TEST_USER,
                cls.HOST,
                cls.TEST_PASSWORD,
            )
        )
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

    @classmethod
    def create_test_user(cls):
        connection = cls._get_superuser_connection()
        connection.cursor().execute(
            "CREATE USER {user} WITH PASSWORD '{password}';".format(
                **{
                    'user': cls.TEST_USER,
                    'password': cls.TEST_PASSWORD,
                }
            )
        )
        connection.close()

    @classmethod
    def create_test_database(cls):
        connection = cls._get_superuser_connection()
        connection.cursor().execute(
            "CREATE DATABASE {database};".format(
                **{
                    'database': cls.TEST_DATABASE,
                }
            )
        )
        connection.close()

    @classmethod
    def grant_test_database_to_test_user(cls):
        connection = cls._get_superuser_connection()
        connection.cursor().execute(
            "GRANT ALL PRIVILEGES ON DATABASE {database} TO {user};".format(
                **{
                    'user': cls.TEST_USER,
                    'database': cls.TEST_DATABASE,
                }
            )
        )
        connection.close()

    @classmethod
    def drop_test_user(cls):
        connection = cls._get_superuser_connection()
        connection.cursor().execute(
            "DROP USER IF EXISTS {user};".format(
                **{
                    'user': cls.TEST_USER,
                }
            )
        )
        connection.close()

    @classmethod
    def drop_test_database(cls):
        connection = cls._get_superuser_connection()
        c = connection.cursor()
        c.execute("select * from pg_stat_activity;")
        connection.cursor().execute(
            "DROP DATABASE IF EXISTS {database};".format(
                **{
                    'database': cls.TEST_DATABASE,
                }
            )
        )
        connection.close()

    @classmethod
    def create_schema(cls, schema_name):
        connection = cls._get_test_database_connection()
        connection.cursor().execute(
            "CREATE SCHEMA {schema_name};".format(
                **{
                    'schema_name': schema_name,
                }
            )
        )
        connection.close()

    @classmethod
    def drop_schema(cls, schema_name):
        connection = cls._get_test_database_connection()
        connection.cursor().execute(
            "DROP SCHEMA IF EXISTS {schema_name};".format(
                **{
                    'schema_name': schema_name,
                }
            )
        )
        connection.close()

    @classmethod
    def clear_test_database(cls):
        raise NotImplementedError()

    @classmethod
    def setup_test_database(cls):
        cls.teardown_test_database()
        cls.create_test_user()
        cls.create_test_database()
        cls.grant_test_database_to_test_user()

    @classmethod
    def teardown_test_database(cls):
        cls.drop_test_database()
        cls.drop_test_user()
