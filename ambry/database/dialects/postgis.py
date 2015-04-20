__author__ = 'eric'


from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2


class PostgisDialect(PGDialect_psycopg2):

    """Trivial Postgis dialect. This is primarily used to allow postgis databases to use the
    'postgis' dialect name, so that the orm.Geometery type variants work """
    name = 'postgis'
