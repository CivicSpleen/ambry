
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from .sqlite import SqliteDatabase, SqliteAttachmentMixin

class SpatialiteDatabase(SqliteDatabase, SqliteAttachmentMixin):

    def create(self):

        super(SpatialiteDatabase, self).create()

        # Will fail if Spatialite is not installed, which on Linuc
        # depends on the load_extension call in database.geo._on_connect_geo()
        self.engine.execute("SELECT InitSpatialMetaData();")


    def drop(self):
        import os
        try:
            os.remove(self.path)
        except OSError:
            pass

        self.create()


    def _on_create_connection(self, connection):
        '''Called from get_connection() to update the database'''
        super(SpatialiteDatabase, self)._on_create_connection(connection)

    def _on_create_engine(self, engine):
        from sqlalchemy import event
        from .geo import _on_connect_geo

        super(SpatialiteDatabase, self)._on_create_engine(engine)

        event.listen(self._engine, 'connect', _on_connect_geo)

class SpatialiteWarehouseDatabase(SpatialiteDatabase):

    pass
