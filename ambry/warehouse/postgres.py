"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from ..dbexceptions import DependencyError
from . import Warehouse
from ..library.database import LibraryDb


class PostgresWarehouse(Warehouse):

    drop_view_sql = 'DROP VIEW  IF EXISTS "{name}" CASCADE'

    def create(self):
        self.database.create()
        self.database.connection.execute(
            'CREATE SCHEMA IF NOT EXISTS library;')
        self.library.database.create()

    def table_meta(self, identity, table_name):
        """Get the metadata directly from the database.

        This requires that table_name be the same as the table as it is
        in stalled in the database

        """

        assert identity.is_partition

        self.library.database.session.execute("SET search_path TO library")

        return super(PostgresWarehouse, self).table_meta(identity, table_name)

    def _ogr_args(self, partition):

        db = self.database

        ogr_dsn = (
            "PG:'dbname={dbname} user={username} host={host} password={password}'" .format(
                username=db.username,
                password=db.password,
                host=db.server,
                dbname=db.dbname))

        return ["-f PostgreSQL", ogr_dsn,
                partition.database.path,
                "--config PG_USE_COPY YES"]

    def drop_user(self, u):
        e = self.database.connection.execute

        try:
            e("DROP SCHEMA {} CASCADE;".format(u))
        except:
            pass

        try:
            e("DROP OWNED BY {}".format(u))
        except:
            pass

        try:
            e("DROP ROLE {}".format(u))
        except:
            pass

    def create_user(self, user, password):

        e = self.database.connection.execute

        e("CREATE ROLE {} LOGIN PASSWORD '{}'".format(user, password))

        e("CREATE SCHEMA {0} AUTHORIZATION {0};".format(user))

        e("ALTER ROLE {0} SET search_path TO library,public,{0};".format(user))

        # From http://stackoverflow.com/a/8247052
        e("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}".format(user))
        e("""ALTER DEFAULT PRIVILEGES IN SCHEMA public
             GRANT SELECT ON TABLES  TO {}; """.format(user))

        e("GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA public TO {}".format(
            user))

        e("""ALTER DEFAULT PRIVILEGES IN SCHEMA public
          GRANT SELECT, USAGE ON SEQUENCES  TO {}""".format(user))

    def users(self):

        q = """SELECT
            u.usename AS "name",
            u.usesysid AS "id",
            u.usecreatedb AS "createdb",
            u.usesuper AS "superuser"
            FROM pg_catalog.pg_user u
            ORDER BY 1;"""

        return {row['name']: dict(row) for row
                in self.database.connection.execute(q)}
