# coding: utf-8

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


DEFAULT_PG_SUPERUSER = 'postgres'
DEFAULT_PG_SUPERUSER_PASSWORD = 'postgres'
DEFAULT_HOST = 'localhost'

TEST_USER = 'niamoto_test'
TEST_PASSWORD = 'niamoto_test'
TEST_DATABASE = 'niamoto_test'


class TestDatabaseManager:
    """
    Object managing the creation, cleaning and destruction of a test
    database.
    """

    def __init__(self, interactive=True):
        self.superuser = DEFAULT_PG_SUPERUSER
        self.superuser_password = DEFAULT_PG_SUPERUSER_PASSWORD
        self.host = DEFAULT_HOST
        if interactive:
            self.prompt_host()
            self.prompt_superuser()
            self.prompt_superuser_password()

    def prompt_superuser(self):
        _superuser = input(
            "Enter the postgres superuser name (default: '{}'):".format(
                DEFAULT_PG_SUPERUSER,
            )
        )
        if _superuser is not '':
            self.superuser = _superuser

    def prompt_superuser_password(self):
        _superuser_password = input(
            "Enter the postgres superuser password (default '{}'):".format(
                DEFAULT_PG_SUPERUSER_PASSWORD,
            )
        )
        if _superuser_password is not '':
            self.superuser_password = _superuser_password

    def prompt_host(self):
        _host = input(
            "Enter the postgres server host (default: '{}'):".format(
                DEFAULT_HOST,
            )
        )
        if _host is not '':
            self.host = _host

    def _get_superuser_connection(self):
        connection = psycopg2.connect(
            "dbname='{}' user='{}' host='{}' password='{}'".format(
                'postgres',
                self.superuser,
                self.host,
                self.superuser_password
            )
        )
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

    def create_test_user(self):
        connection = self._get_superuser_connection()
        connection.cursor().execute(
            "CREATE USER {user} WITH PASSWORD '{password}';".format(
                **{
                    'user': TEST_USER,
                    'password': TEST_PASSWORD,
                }
            )
        )
        connection.close()

    def create_test_database(self):
        connection = self._get_superuser_connection()
        connection.cursor().execute(
            "CREATE DATABASE {database};".format(**{'database': TEST_DATABASE})
        )
        connection.close()

    def grant_test_database_to_test_user(self):
        connection = self._get_superuser_connection()
        connection.cursor().execute(
            "GRANT ALL PRIVILEGES ON DATABASE {database} TO {user};".format(
                **{
                    'user': TEST_USER,
                    'database': TEST_DATABASE,
                }
            )
        )
        connection.close()

    def drop_test_user(self):
        connection = self._get_superuser_connection()
        connection.cursor().execute(
            "DROP USER IF EXISTS {user};".format(
                **{
                    'user': TEST_USER,
                }
            )
        )
        connection.close()

    def drop_test_database(self):
        connection = self._get_superuser_connection()
        connection.set_isolation_level(0)
        connection.cursor().execute(
            "DROP DATABASE IF EXISTS {database};".format(
                **{
                    'database': TEST_DATABASE,
                }
            )
        )
        connection.close()

    def clear_test_database(self):
        pass

    def setup_test_database(self):
        self.teardown_test_database()
        self.create_test_user()
        self.create_test_database()
        self.grant_test_database_to_test_user()

    def teardown_test_database(self):
        self.drop_test_database()
        self.drop_test_user()
