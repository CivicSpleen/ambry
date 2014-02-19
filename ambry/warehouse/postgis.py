"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from postgres import PostgresWarehouse #@UnresolvedImport


class PostgisWarehouse(PostgresWarehouse):
    

    def create(self):
        super(PostgisWarehouse, self).create()

        self.database.connection.execute('CREATE EXTENSION IF NOT EXISTS postgis')
        self.database.connection.execute('CREATE EXTENSION IF NOT EXISTS postgis_topology;')

        # Actually only for Amazon RDS
        try:  self.database.connection.execute('CREATE EXTENSION IF NOT EXISTS fuzzystrmatch')
        except: pass

    def _ogr_args(self, partition):

        db = self.database

        ogr_dsn = ("PG:'dbname={dbname} user={username} host={host} password={password}'"
                   .format(username=db.username, password=db.password,
                           host=db.server, dbname=db.dbname))

        return ["-f PostgreSQL", ogr_dsn,
                partition.database.path,
                "--config PG_USE_COPY YES"]

    def _install_geo_partition(self, partition):
        
        from ambry.client.exceptions import NotFound
        
        p = self.resolver.get(partition.identity.vname)
        
        for table_name in partition.data.get('tables',[]):
            
            table, meta = self.create_table(partition.identity, table_name)

            self._install_geo_partition_table(partition,table)

    def _install_geo_partition_table(self, partition, table):
        #
        # Use ogr2ogr to copy. 
        #
        import shlex
        
        db = self.database
        
        self.library.database.mark_table_installed(partition.get_table().vid, table.name)
        
    
        args = [
        "-t_srs EPSG:2771",
        "-nlt PROMOTE_TO_MULTI",
        "-nln {}".format(table.name),
        "-progress ",
        "-overwrite",
        "-skipfailures",
        "-f PostgreSQL",
        ("PG:'dbname={dbname} user={username} host={host} password={password}'"
         .format(username=db.username, password=db.password, 
                    host=db.server, dbname=db.dbname)),
        partition.database.path,
        "--config PG_USE_COPY YES"]
        
        def err_output(line):
            self.logger.error(line)
        
        global count
        count = 0
        def out_output(c): 
            global count
            count +=  1
            if count % 10 == 0:
                pct =  (int(count / 10)-1) * 20
                if pct <= 100:
                    self.logger.log("Loading {}%".format(pct))  



        self.logger.log("Loading with: ogr2ogr {}".format(' '.join(args)))
        
        # Need to shlex it b/c the "PG:" part gets bungled otherwise. 
        p = ogr2ogr(*shlex.split(' '.join(args)),  _err=err_output, _out=out_output, _iter=True, _out_bufsize=0)
        p.wait()

        return
        

        
        
        
        
        