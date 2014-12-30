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


        
        
        