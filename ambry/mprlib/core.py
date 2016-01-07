import sqlparse

from ambry.util import get_logger

from .backends.sqlite import SQLiteBackend, _preprocess_sqlite_view, _preprocess_sqlite_index
from .backends.postgresql import PostgreSQLBackend, _preprocess_postgres_index


logger = get_logger(__name__)

# debug logging
#
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


def execute_sql(bundle, asql):
    """ Executes all sql statements from asql.

    Args:
        bundle (FIXME:):
        asql (str): unified sql query - see https://github.com/CivicKnowledge/ambry/issues/140 for details.
    """
    engine_name = bundle.library.database.engine.name
    if engine_name == 'sqlite':
        backend = SQLiteBackend(bundle.library, bundle.library.database.dsn)
        # FIXME: move preprocessors to the backend.
        pipe = [_preprocess_sqlite_view, _preprocess_sqlite_index]
    elif engine_name == 'postgresql':
        backend = PostgreSQLBackend(bundle.library, bundle.library.database.dsn)
        pipe = [_preprocess_postgres_index]
    else:
        raise Exception('Do not know backend for {} database.'.format(engine_name))
    connection = backend._get_connection()
    if engine_name == 'postgresql':
        with connection.cursor() as cursor:
            # TODO: Move to backend methods.
            cursor.execute('SET search_path TO {};'.format(bundle.library.database._schema))
    try:
        statements = sqlparse.parse(sqlparse.format(asql, strip_comments=True))
        for statement in statements:
            statement_str = statement.to_unicode()
            for preprocessor in pipe:
                statement_str = preprocessor(statement_str, bundle.library, backend, connection)
            if statement_str.strip():
                backend.query(connection, statement_str, fetch=False)
    finally:
        backend.close()
