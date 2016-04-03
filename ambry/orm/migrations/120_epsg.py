# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration

class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        try:
            connection.execute('ALTER TABLE datasources ADD COLUMN ds_epsg INTEGER')
        except:
            # Some of the bundles fail here, don't know why.
            pass

        connection.execute('ALTER TABLE partitions ADD COLUMN p_epsg INTEGER')

        

    def _migrate_postgresql(self, connection):
        connection.execute('ALTER TABLE ambrylib.datasources ADD COLUMN ds_epsg INTEGER')
        connection.execute('ALTER TABLE ambrylib.partitions ADD COLUMN p_epsg INTEGER')