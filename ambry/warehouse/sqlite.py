"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ..dbexceptions import DependencyError
from relational import RelationalWarehouse
from ..library.database import LibraryDb
from . import ResolutionError


class SqliteWarehouse(RelationalWarehouse):


    ##
    ## Datasets and Bundles
    ##

    def install(self, ref):
        pass

        bundle, partition, tables = self._setup_install(ref)

        print bundle.identity.fqname
        print partition.identity.fqname
        print tables
     
    def _install_partition(self, partition):

        from ambry.client.exceptions import NotFound
        
        self.logger.log('install_partition_csv {}'.format(partition.identity.name))

        pdb = partition.database


        
    def _install_csv_url(self, table, url):
        
        self.logger.log('install_csv_url {}'.format(url))

        cmd =  self._copy_command(table.name, url)
        self.logger.log('installing with command: {} '.format(cmd))
        r = self.database.connection.execute(cmd)
                
        #self.logger.log('installed_csv_url {}'.format(url)) 
        
        r = self.database.connection.execute('commit')

    def remove_by_name(self,name):
        '''Call the parent, then remove CSV partitions'''
        from ..bundle import LibraryDbBundle
        from ..identity import PartitionNameQuery

        super(SqliteWarehouse, self).remove_by_name(name)

        dataset = self.get(name)

        if dataset.partition:
            b = LibraryDbBundle(self.library.database, dataset.vid)
            p = b.partitions.find(PartitionNameQuery(id_=dataset.partition.id_))
 
            for p in p.get_csv_parts():
                super(SqliteWarehouse, self).remove_by_name(p.identity.vname)

        def _install_geo_partition_table(self, partition, table):
            #
            # Use ogr2ogr to copy.
            #
            import shlex

            db = self.database

            self.library.database.install_table(partition.get_table().vid, table.name)

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
                count += 1
                if count % 10 == 0:
                    pct = (int(count / 10) - 1) * 20
                    if pct <= 100:
                        self.logger.log("Loading {}%".format(pct))


            self.logger.log("Loading with: ogr2ogr {}".format(' '.join(args)))

            # Need to shlex it b/c the "PG:" part gets bungled otherwise.
            p = ogr2ogr(*shlex.split(' '.join(args)), _err=err_output, _out=out_output, _iter=True, _out_bufsize=0)
            p.wait()

            return

