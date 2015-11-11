# -*- coding: utf-8 -*-
from ambry.orm.database import BaseMigration

# Adds new column column1 to the datasets table.


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        query = '''
            ALTER TABLE datasets ADD COLUMN column1 INTEGER;
        '''
        connection.execute(query)

    def _migrate_postgresql(self, connection):
        query = '''
            ALTER TABLE ambrylib.datasets ADD COLUMN column1 INTEGER;
        '''
        connection.execute(query)
