"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from relational import RelationalWarehouse #@UnresolvedImport
from ..library.database import LibraryDb


class PostgresWarehouse(RelationalWarehouse):

    
    def create(self):
        self.database.create()
        self.database.connection.execute('CREATE SCHEMA IF NOT EXISTS library;')
        self.library.database.create()
       
       
    def drop_user(self, u):
        e = self.database.connection.execute
        
        
        try: e("DROP SCHEMA {} CASCADE;".format(u))
        except: pass
        
        try: e("DROP OWNED BY {}".format(u))
        except: pass
        
        try: e("DROP ROLE {}".format(u))  
        except: pass
              
    def create_user(self, user, password):
        
        e = self.database.connection.execute
        
        
        e("CREATE ROLE {} LOGIN PASSWORD '{}'".format(user, password))
        
        e("CREATE SCHEMA {0} AUTHORIZATION {0};".format(user))
        
        e("ALTER ROLE {0} SET search_path TO library,public,{0};".format(user))
        
        # From http://stackoverflow.com/a/8247052
        e("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}".format(user))
        e("""ALTER DEFAULT PRIVILEGES IN SCHEMA public 
             GRANT SELECT ON TABLES  TO {}; """.format(user))

        e("GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA public TO {}".format(user))

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
        
        return { row['name']:dict(row) for row 
                in self.database.connection.execute(q) }


    def table_meta(self, identity, table_name):
        '''Get the metadata directly from the database. This requires that
        table_name be the same as the table as it is in stalled in the database'''

        assert identity.is_partition

        self.library.database.session.execute("SET search_path TO library")

        return super(PostgresWarehouse, self).table_meta(identity, table_name)


    def remove_by_name(self,name):
        '''Call the parent, then remove CSV partitions'''
        from ..bundle import LibraryDbBundle
        from ..identity import PartitionNameQuery

        super(PostgresWarehouse, self).remove_by_name(name)

        dataset = self.get(name)

        if dataset.partition:
            b = LibraryDbBundle(self.library.database, dataset.vid)
            p = b.partitions.find(PartitionNameQuery(id_=dataset.partition.id_))
 
            for p in p.get_csv_parts():
                super(PostgresWarehouse, self).remove_by_name(p.identity.vname)

    def _ogr_args(self, partition):

        db = self.database

        ogr_dsn = ("PG:'dbname={dbname} user={username} host={host} password={password}'"
                   .format(username=db.username, password=db.password,
                           host=db.server, dbname=db.dbname))

        return ["-f PostgreSQL", ogr_dsn,
                partition.database.path,
                "--config PG_USE_COPY YES"]

    def _copy_command(self, table, url):

        template = """COPY "public"."{table}"  FROM  PROGRAM 'curl -s -L --compressed "{url}"'  WITH ( FORMAT csv )"""

        return template.format(table=table, url=url)


    def load_local(self, partition, source_table_name, dest_table_name, where=None):
        return self.load_insert(partition, source_table_name, dest_table_name, where=where)


    def install_view(self, name, sql, data=None):
        import time

        assert name
        assert sql

        t = self.orm_table_by_name(name)

        if t and t.data.get('sql') == sql:
            self.logger.info("Skipping view {}; SQL hasn't changed".format(name))
        else:
            self.logger.info('Installing view {}'.format(name))


        data = data if data else {}
        data['type'] = 'view'

        data['sql'] = sql
        data['updated'] = time.time()
        sqls = ['DROP VIEW  IF EXISTS "{name}" CASCADE'.format(name=name),
        'CREATE VIEW "{name}" AS {sql}'.format(name=name, sql=sql)]

        try:
            for sql in sqls:
                self.database.connection.execute(sql)

            self.install_table(name, data=data)


        except Exception as e:
            self.logger.error("Failed to install view: \n{}".format(sql))
            raise

    def install_material_view(self, name, sql, clean=False, data=None):
        from pysqlite2.dbapi2 import OperationalError
        import time

        drop, data = self._install_material_view(name, sql, clean=clean, data = data)

        if drop:
            self.database.connection.execute('DROP TABLE IF EXISTS "{}"'.format(name))

        if not data:
            return False


        sql = """
        CREATE TABLE {name} AS {sql}
        """.format(name=name, sql=sql)

        try:
            self.database.connection.execute(sql)

        except OperationalError as e:
            if 'exists' not in str(e).lower():
                raise

            self.logger.info('mview_exists {}'.format(name))
            # Ignore if it already exists.


        self.install_table(name, data=data)

