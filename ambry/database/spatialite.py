
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from .sqlite import SqliteDatabase, SqliteAttachmentMixin

class SpatialiteDatabase(SqliteDatabase, SqliteAttachmentMixin):

    def create(self):

        super(SpatialiteDatabase, self).create()

        self.engine.execute("SELECT InitSpatialMetaData();")


    def drop(self):
        import os
        try:
            os.remove(self.path)
        except OSError:
            pass

        self.create()


    def _on_connect(self):
        from sqlite import _on_connect_update_sqlite_schema
        from geo import _on_connect_geo
        '''Called from engine() to update the database'''
        _on_connect_update_sqlite_schema(self.connection)
        _on_connect_geo(self.connection)