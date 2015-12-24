from semantic_version import Version

import ambry_sources
from ambry_sources.med import sqlite as sqlite_med

AMBRY_SOURCES_VERSION = getattr(ambry_sources, '__version__', None) or ambry_sources.__meta__.__version__


def assert_sqlite_index(connection, partition, column):
    """ Checks either the given column indexed. Raises assertion error if not. """
    table = sqlite_med.table_name(partition.vid) + '_v'
    cursor = connection.cursor()
    query = 'EXPLAIN QUERY PLAN SELECT * FROM {} WHERE {} > 1;'.format(table, column)
    result = cursor.execute(query).fetchall()
    assert 'USING INDEX' in result[0][-1]


def assert_valid_ambry_sources(version):
    """ Checks ambry_sources version. Raises assertion if ambry_sources version is less then given version."""
    if Version(AMBRY_SOURCES_VERSION) < Version(version):
        raise AssertionError('Need ambry_sources >= {}. Update your installation.'.format(version))
