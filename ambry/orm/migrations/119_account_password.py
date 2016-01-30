# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        connection.execute('ALTER TABLE datasources ADD COLUMN ds_epsg INTEGER')
        

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER TABLE ambrylib.accounts ADD COLUMN ac_password VARCHAR')
        