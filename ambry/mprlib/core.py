import sqlparse

from ambry.util import get_logger

logger = get_logger(__name__)

# debug logging
#
# import logging
# logger = get_logger(__name__, level=logging.DEBUG, propagate=False)


def execute_sql(library, asql):
    """ Executes all sql statements from asql.

    Args:
        library (library.Library):
        asql (str): unified sql query - see https://github.com/CivicKnowledge/ambry/issues/140 for details.
    """
    engine_name = library.database.engine.name
    if engine_name == 'sqlite':
        from .backends.sqlite import SQLiteBackend, _preprocess_sqlite_view, _preprocess_sqlite_index
        backend = SQLiteBackend(library, library.database.dsn)
        # TODO: move preprocessors to the backend.
        pipe = [_preprocess_sqlite_view, _preprocess_sqlite_index]
        connection = backend._get_connection()
    elif engine_name == 'postgresql':
        from .backends.postgresql import PostgreSQLBackend, _preprocess_postgres_index

        backend = PostgreSQLBackend(library, library.database.dsn)
        pipe = [_preprocess_postgres_index]
        connection = backend._get_connection()
        with connection.cursor() as cursor:
            # TODO: Move to backend methods.
            cursor.execute('SET search_path TO {};'.format(library.database._schema))
    else:
        raise Exception('Do not know backend for {} database.'.format(engine_name))
    try:
        statements = sqlparse.parse(sqlparse.format(asql, strip_comments=True))
        for statement in statements:
            statement_str = statement.to_unicode()
            for preprocessor in pipe:
                statement_str = preprocessor(statement_str, library, backend, connection)
            if statement_str.strip():
                backend.query(connection, statement_str, fetch=False)
    finally:
        backend.close()
