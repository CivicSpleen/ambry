__author__ = 'eric'


from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite


class SpatialiteDialect(SQLiteDialect_pysqlite):
    """Trivial spatialite dialect. This is primarily used to allow postgis databases to use the
    'postgis' dialect name, so that the orm.Geometery type variants work """

    name = 'spatialite'




