
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from .sqlite import SqliteDatabase

class SpatialiteDatabase(SqliteDatabase):

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


    @property
    def engine(self):
        from . import _on_connect_geo
        return self._get_engine(_on_connect_geo)
