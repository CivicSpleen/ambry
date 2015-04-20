"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from postgres import PostgresDatabase

import dialects.postgis


class PostgisDatabase(PostgresDatabase):

    is_geo = True

    @property
    def munged_dsn(self):
        return self.dsn.replace('postgis:', 'postgis:')
