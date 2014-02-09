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


    def _ogr_args(self, partition):

        return [
            "-f SQLite ",self.database.path,
            "-gt 65536",
            partition.database.path,
            "-dsco SPATIALITE=no"]


    def load_local(self, partition, table_name):
        self.load_attach(partition, table_name)

    def load_attach(self, partition, table_name):
        from ..database.inserter import ValueInserter

        self.logger.info('load_attach {}'.format(partition.identity.name))

        p_vid = partition.identity.vid
        d_vid = partition.identity.as_dataset().vid

        source_table_name = table_name
        dest_table_name =  self.augmented_table_name(d_vid, table_name)

        with self.database.engine.begin() as conn:
            atch_name = self.database.attach(partition, conn=conn)
            self.logger.info('load_attach {}'.format(partition.database.path))
            self.database.copy_from_attached( table=(source_table_name, dest_table_name),
                                              on_conflict='REPLACE',
                                              name=atch_name, conn=conn)

        self.logger.info('done {}'.format(partition.identity.vname))


    def load_remote(self, partition, table_name, urls):

        import shlex
        from sh import ambry_load_sqlite, ErrorReturnCode_1

        self.logger.info('load_remote {} '.format(partition.identity.vname, table_name))

        d_vid = partition.identity.as_dataset().vid

        a_table_name = self.augmented_table_name(d_vid, table_name)

        for url in urls:

            try:
                p = ambry_load_sqlite(url, self.database.path, a_table_name,
                                      _err=self.logger.error, _out=self.logger.info )
                p.wait()
            except Exception as e:
                self.logger.error("Failed to load: {} {}: {}".format(partition.identity.vname, table_name, e.message))


class SpatialiteWarehouse(SqliteWarehouse):

    def _ogr_args(self, partition):

        return [
            "-f SQLite ", self.database.path,
            "-gt 65536",
            partition.database.path,
            "-dsco SPATIALITE=yes"]