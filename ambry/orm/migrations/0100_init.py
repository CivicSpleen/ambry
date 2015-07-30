# -*- coding: utf-8 -*-
from ambry.orm.database import BaseMigration

# This is initial migration and actually does nothing. Leave it here as an example and for testing.


class Migration(BaseMigration):

    is_ready = True

    def _migrate_sqlite(self, connection):
        pass

    def _migrate_postgresql(self, connection):
        pass
