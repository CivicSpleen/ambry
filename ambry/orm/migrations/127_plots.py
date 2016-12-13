# -*- coding: utf-8 -*-'

from ambry.orm.database import BaseMigration


class Migration(BaseMigration):

    def _migrate_sqlite(self, connection):
        from ambry.orm import Plot
        self.create_table(Plot, connection)


    def _migrate_postgresql(self, connection):
        from ambry.orm import Plot
        from ambry.orm.database import POSTGRES_SCHEMA_NAME
        self.create_table(Plot, connection, POSTGRES_SCHEMA_NAME)
