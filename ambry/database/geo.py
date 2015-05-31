"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from inserter import InserterInterface
from .partition import PartitionDb
from ..partition.geo import GeoPartitionName
from sqlalchemy.types import LargeBinary, BINARY


class GeoDb(PartitionDb):

    EXTENSION = GeoPartitionName.PATH_EXTENSION

    MIN_NUMBER_OF_TABLES = 5  # Used in is_empty

    is_geo = True

    def __init__(self, bundle, partition, base_path, **kwargs):
        """"""

        kwargs['driver'] = 'spatialite'

        super(GeoDb, self).__init__(bundle, partition, base_path, **kwargs)

    @classmethod
    def make_path(cls, container):
        return container.path + cls.EXTENSION

    def _post_create(self):
        self.connection.execute("SELECT InitSpatialMetaData();")

    def recover_geometry(
            self,
            table_name,
            column_name,
            geometry_type,
            srs=None):
        from ..geo.util import recover_geometry

        recover_geometry(
            self.connection,
            table_name,
            column_name,
            geometry_type,
            srs=srs)

    def _on_create_connection(self, connection):
        """Called from get_connection() to update the database."""
        super(GeoDb, self)._on_create_connection(connection)

    def _on_create_engine(self, engine):
        from sqlalchemy import event

        super(GeoDb, self)._on_create_engine(engine)

        event.listen(self._engine, 'connect', _on_connect_geo)


class SpatialiteWarehouseDatabase(GeoDb):
    pass


def _on_connect_geo(dbapi_con, con_record):
    """ISSUE some Sqlite pragmas when the connection is created."""
    from ..util import RedirectStdStreams
    from sqlite import _on_connect_bundle as ocb
    from ambry.orm import DatabaseError
    from pysqlite2.dbapi2 import OperationalError

    ocb(dbapi_con, con_record)

    # NOTE ABOUT journal_mode = WAL: it improves concurency, but has some
    # downsides.
    # See http://sqlite.org/wal.html

    dbapi_con.execute('PRAGMA page_size = 8192')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 50000')
    dbapi_con.execute('PRAGMA foreign_keys = OFF')
    dbapi_con.execute('PRAGMA journal_mode = WAL')
    dbapi_con.execute('PRAGMA synchronous = OFF')

    def load_extension():
        try:
            dbapi_con.execute('select spatialite_version()')
            return
        except:
            try:
                dbapi_con.enable_load_extension(True)
            except AttributeError:
                raise

        # This is so wrong, but I don't know what's right.
        # ( My code has become a Country song. )

        libs = [
            "select load_extension('/usr/lib/x86_64-linux-gnu/libspatialite')",
            "select load_extension('/usr/lib/libspatialite.so')",
            "select load_extension('/usr/lib/libspatialite.so.3')",
            "select "
            "load_extension('/usr/lib/x86_64-linux-gnu/libspatialite.so.5')",

        ]

        for l in libs:
            try:
                # Spatialite prints its version header always, this supresses
                # it.
                with RedirectStdStreams():
                    dbapi_con.execute(l)

                return
            except OperationalError:
                continue

        raise DatabaseError(
            "Could not load the spatialite extension. Tried: {}".format(libs))

    load_extension()
