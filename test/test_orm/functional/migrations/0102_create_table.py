# -*- coding: utf-8 -*-
from ambry.orm.database import BaseMigration

# This is initial migration and actually does nothing. Leave it here as an example and for testing.


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        query = '''
            CREATE TABLE table1(
                column1 INTEGER
            );
        '''
        connection.execute(query)

    def _migrate_postgresql(self, connection):
        query = '''
            CREATE TABLE ambrylib.table1(
                column1 INTEGER
            );
        '''
        connection.execute(query)
