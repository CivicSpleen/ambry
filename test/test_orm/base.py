# -*- coding: utf-8 -*-
import unittest
from six.moves.urllib.parse import urlparse

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from ambry.run import get_runconfig

MISSING_POSTGRES_CONFIG_MSG = 'PostgreSQL is not configured properly. Add postgresql-test '\
    'to the database config of the ambry config.'
# example of the config - dsn: postgresql+psycopg2://ambry:secret@127.0.0.1/ambry


class BasePostgreSQLTest(unittest.TestCase):
    """ Base class for database tests who requires postgresql connection. """

    def setUp(self):
        conf = get_runconfig()

        if 'database' in conf.dict and 'postgresql-test' in conf.dict['database']:
            dsn = conf.dict['database']['postgresql-test']
            parsed_url = urlparse(dsn)
            db_name = parsed_url.path.replace('/', '')
            self.postgres_dsn = parsed_url._replace(path='postgres').geturl()
            self.postgres_test_db = '{}_test_db1ae'.format(db_name)
            self.postgres_test_dsn = parsed_url._replace(path=self.postgres_test_db).geturl()
        else:
            self.postgres_dsn = None
            self.postgres_test_dsn = None
            self.postgres_test_db = None

    def tearDown(self):
        # drop test database
        if getattr(self, '_active_pg_connection', None):
            self._active_pg_connection.execute('rollback')
            self._active_pg_connection.detach()
            self._active_pg_connection.close()
            self._active_pg_connection = None

            # droop test database;
            engine = create_engine(self.postgres_dsn)
            connection = engine.connect()
            connection.execute('commit')
            connection.execute('DROP DATABASE {};'.format(self.postgres_test_db))
            connection.execute('commit')
            connection.close()

    def pg_connection(self):
        # creates test database and returns postgres connection to that database.
        postgres_user = 'ambry'
        if not self.postgres_dsn:
            raise Exception(MISSING_POSTGRES_CONFIG_MSG)

        # connect to postgres database because we need to create database for tests.
        engine = create_engine(self.postgres_dsn)
        connection = engine.connect()

        # we have to close opened transaction.
        connection.execute('commit')

        # drop test database created by previuos run (control + c case).
        try:
            connection.execute('DROP DATABASE {};'.format(self.postgres_test_db))
            connection.execute('commit')
        except:
            connection.execute('rollback')

        # create test database
        query = 'CREATE DATABASE {} OWNER {} template template0 encoding \'UTF8\';'\
            .format(self.postgres_test_db, postgres_user)
        connection.execute(query)
        connection.execute('commit')
        connection.close()

        # now create connection for tests. Disable pooling to make close() easier.
        self.pg_engine = create_engine(self.postgres_test_dsn, poolclass=NullPool)
        pg_connection = self.pg_engine.connect()
        self._active_pg_connection = pg_connection
        return pg_connection
