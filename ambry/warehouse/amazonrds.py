"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from postgis import PostgisWarehouse #@UnresolvedImport
from sh import ogr2ogr #@UnresolvedImport

class PostgresRDSWarehouse(PostgisWarehouse):
    

    def create(self):
        super(PostgresRDSWarehouse, self).create()

        #self.database.connection.execute('CREATE EXTENSION IF NOT EXISTS fuzzystrmatch')

    # Don't have access to COPY, since not actual superuser. 
    def _install_partition(self, partition):
        self._install_partition_insert(partition)