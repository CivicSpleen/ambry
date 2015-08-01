# -*- coding: utf-8 -*-
from ambry.orm.database import BaseMigration

# This is initial migration and actually does nothing. Leave it here as an example and for testing.


class Migration(BaseMigration):

    def _migrate(self, connection):
        # create table is the same for sqlite and _migrate_postgresql
        query = '''
            CREATE TABLE table1(
                column1 INTEGER
            );
        '''
        connection.execute(query)

    def _migrate_sqlite(self, connection):
        self._migrate(connection)

    def _migrate_postgresql(self, connection):
        self._migrate(connection)
