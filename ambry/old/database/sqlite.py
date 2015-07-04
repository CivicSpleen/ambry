
"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""

from __future__ import absolute_import
# @UnresolvedImport
from .relational import RelationalBundleDatabaseMixin, RelationalDatabase
import os
from ambry.util import get_logger

# import logging

global_logger = get_logger(__name__)
# logger.setLevel(logging.DEBUG)


class SqliteAttachmentMixin(object):

    def attach(self, id_, name=None, conn=None):
        """Attach another sqlite database to this one.

        Args:
            id_ Itentifies the other database. May be a:
                Path to a database
                Identitfier object, for a undle or partition
                Datbase or PartitionDb object

            name. Name by which to attach the database. Uses a random
            name if None

        The method iwll also store the name of the attached database, which
        will be used in copy_to() and copy_from() if a name is not provided

        Returns:
            name by whih the database was attached

        """
        from ..identity import Identity
        from ..partition import PartitionInterface
        from ..bundle import Bundle
        from .partition import PartitionDb

        if isinstance(id_, basestring):
            #  Strings are path names
            path = id_
        elif isinstance(id_, Identity):
            path = id_.path
        elif isinstance(id_, PartitionDb):
            path = id_.path
        elif isinstance(id_, PartitionInterface):
            path = id_.database.path
        elif isinstance(id_, Bundle):
            path = id_.database.path
        elif id_ is None:
            raise ValueError("Can't attach: None given for parameter id_")
        else:
            raise ValueError(
                "Can't attach: Don't understand id_: {}".format(
                    repr(id_)))

        if name is None:
            import random
            import string
            name = ''.join(random.choice(string.letters) for i in xrange(10))  # @UnusedVariable

        q = """ATTACH DATABASE '{}' AS '{}' """.format(path, name)

        if not conn:
            conn = self.connection

        conn.execute(q)

        self._last_attach_name = name

        self._attachments.add(name)

        return name

    def detach(self, name=None):
        """Detach databases.

        Args:
            name. Name of database to detach. If None, detatch all

        """

        if name is None:
            name = self._last_attach_name

        self.connection.execute("""DETACH DATABASE {} """.format(name))

        self._attachments.remove(name)

    def copy_from_attached(self, table,columns=None, name=None, on_conflict='ABORT',
            where=None,conn=None,copy_n=None):
        """Copy from this database to an attached database.

        Args:
            map_. a dict of k:v pairs for the values in this database to
            copy to the remote database. If None, copy all values

            name. The attach name of the other datbase, from self.attach()

            on_conflict. How conflicts should be handled

            where. An additional where clause for the copy.

        """

        if name is None:
            name = self._last_attach_name

        f = {
            'db': name,
            'on_conflict': on_conflict,
            'from_columns': '*',
            'to_columns': ''}

        if isinstance(table, basestring):
            # Copy all fields between tables with the same name
            f['from_table'] = table
            f['to_table'] = table

        elif isinstance(table, tuple):
            # Copy all fields between two tables with different names
            f['from_table'] = table[0]
            f['to_table'] = table[1]
        else:
            raise Exception("Unknown table type " + str(type(table)))

        if columns is None:
            pass
        elif isinstance(columns, dict):
            f['from_columns'] = ','.join([k for k, v in columns.items()])
            f['to_columns'] = '(' + \
                ','.join([v for k, v in columns.items()]) + ')'

        q = """INSERT OR {on_conflict} INTO {to_table} {to_columns}
               SELECT {from_columns} FROM {db}.{from_table}""".format(**f)

        if where is not None:
            q = q + " " + where.format(**f)

        if copy_n:
            q += ' LIMIT {}'.format(copy_n)

        if conn:
            conn.execute(q)
        else:
            with self.engine.begin() as conn:
                conn.execute(q)


class SqliteDatabase(RelationalDatabase):

    EXTENSION = '.db'
    SCHEMA_VERSION = 29

    _lock = None

    def __init__(self, dbname, memory=False, **kwargs):
        """"""
        # For database bundles, where we have to pass in the whole file path
        if memory is False:
            if not dbname:
                raise ValueError("Must have a dbname")

            if dbname[0] != '/':
                dbname = os.path.join(os.getcwd(), dbname)

            base_path, ext = os.path.splitext(dbname)

            if ext and ext != self.EXTENSION:
                raise Exception("Bad extension to file '{}': '{}'. Expected: {}".format(dbname, ext, self.EXTENSION))

            self.base_path = base_path

        self._last_attach_name = None
        self._attachments = set()

        self.memory = memory

        if 'driver' not in kwargs:
            kwargs['driver'] = 'sqlite'

        super(SqliteDatabase, self).__init__(dbname=self.path, **kwargs)

    @property
    def path(self):
        if self.memory:
            return ':memory:'
        else:
            return (self.base_path + self.EXTENSION).replace('//', '/')

    @property
    def md5(self):
        from ambry.util import md5_for_file
        return md5_for_file(self.path)

    @property
    def lock_path(self):
        return self.base_path

    def lock(self):
        """Create an external file lock for the bundle database."""

        from lockfile import FileLock, AlreadyLocked  # , LockTimeout
        import time
        import traceback
        from ..dbexceptions import LockedFailed

        if self._lock:
            tb = traceback.extract_stack()[-5:-4][0]
            global_logger.debug(
                "Already has bundle lock from {}:{}".format(
                    tb[0],
                    tb[1]))
            return

        self._lock = FileLock(self.lock_path)

        for i in range(10):
            try:
                tb = traceback.extract_stack()[-5:-4][0]
                self._lock.acquire(-1)
                global_logger.debug(
                    "Acquired bundle lock from {}:{}".format(
                        tb[0],
                        tb[1]))
                return
            except AlreadyLocked:
                global_logger.debug("Waiting for bundle lock")
                time.sleep(1)

        raise LockedFailed("Failed to acquire lock on {}".format(self.lock_path))
        # self._lock = None

    def unlock(self):
        """Release the external lock on the external database."""
        global_logger.debug("Released bundle lock")
        if self._lock is not None:
            self._lock.release()
            self._lock = None

    def break_lock(self):
        from lockfile import FileLock

        lock = FileLock(self.lock_path)

        if lock.is_locked():
            lock.break_lock()

    def require_path(self):
        if not self.memory:

            dir = os.path.dirname(self.base_path)

            if dir and not os.path.exists(dir):
                os.makedirs(os.path.dirname(self.base_path))

    @property
    def version(self):
        v = self.connection.execute('PRAGMA user_version').fetchone()[0]

        try:
            return int(v)
        except:
            return 0

    def _on_create_connection(self, connection):
        """Called from get_connection() to update the database."""
        pass

    def _on_create_engine(self, engine):
        """Called just after the engine is created."""
        pass

    def get_connection(self, check_exists=True):
        """Return an SqlAlchemy connection, but allow for existence check,
        which uses os.path.exists."""

        if not os.path.exists(self.path) and check_exists and not self.memory:
            from ambry.orm.exc import DatabaseMissingError

            raise DatabaseMissingError(
                "Trying to make a connection to a sqlite database " +
                "that does not exist.  path={}".format(
                    self.path))

        return super(SqliteDatabase, self).get_connection(check_exists)

    def _create(self):
        """Need to ensure the database exists before calling for the
        connection, but the connection expects the database to exist first, so
        we create it here."""

        from sqlalchemy import create_engine

        dir_ = os.path.dirname(self.path)

        if not os.path.exists(dir_):
            os.makedirs(dir_)

        engine = create_engine(self.dsn, echo=False)
        connection = engine.connect()
        try:
            connection.execute(
                "PRAGMA user_version = {}".format(
                    self.SCHEMA_VERSION))
        except Exception as e:
            e.args = ("Failed to open database {}".format(self.dsn),)
            raise e

        connection.close()
        engine.dispose()

    MIN_NUMBER_OF_TABLES = 1

    def is_empty(self):

        if not self.memory and not os.path.exists(self.path):
            return True

        if self.version >= 12:

            if 'config' not in self.inspector.get_table_names():
                return True
            else:
                return False
        else:

            tables = self.inspector.get_table_names()

            if tables and len(tables) < self.MIN_NUMBER_OF_TABLES:
                return True
            else:
                return False

    def clean(self):
        """Remove all files generated by the build process."""
        if os.path.exists(self.path):
            os.remove(self.path)
        self.unlock()

    def delete(self):

        if os.path.exists(self.path):

            self.unlock()

            files = [
                self.path,
                self.path + "-wal",
                self.path + "-shm",
                self.path + "-journal"]

            for f in files:
                if os.path.exists(f):
                    os.remove(f)

    def add_view(self, name, sql):

        e = self.connection.execute

        e('DROP VIEW IF EXISTS {}'.format(name))

        e('CREATE VIEW {} AS {} '.format(name, sql))

    def load(self, a, table=None, encoding='utf-8', caster=None, logger=None):
        """Load the database from a CSV file."""

        # return self.load_insert(a,table, encoding=encoding, caster=caster,
        # logger=logger)
        return self.load_shell(a,table,encoding=encoding,caster=caster, logger=logger)

    def load_insert(self, a,table=None, encoding='utf-8', caster=None, logger=None):
        from ..partition import PartitionInterface
        from ..database.csv import CsvDb
        from ..dbexceptions import ConfigurationError
        import time

        if isinstance(a, PartitionInterface):
            db = a.database
        elif isinstance(a, CsvDb):
            db = a
        else:
            raise ConfigurationError("Can't use this type: {}".format(type(a)))

        start = time.clock()
        count = 0
        with self.inserter(table, caster=caster) as ins:
            for row in db.reader(encoding=encoding):
                count += 1

                if logger:
                    logger("Load row {}:".format(count))

                ins.insert(row)

        diff = time.clock() - start
        return count, diff

    def load_shell(self, a, table, encoding='utf-8', caster=None, logger=None):
        from ..partition import PartitionInterface
        from ..database.csv import CsvDb
        from ..dbexceptions import ConfigurationError
        import time
        import subprocess
        # import uuid
        # import os

        if isinstance(a, PartitionInterface):
            db = a.database
        elif isinstance(a, CsvDb):
            db = a
        else:
            raise ConfigurationError("Can't use this type: {}".format(type(a)))

        try:
            table_name = table.name
        except AttributeError:
            table_name = table

        # sql_file = temp_file_name()

        sql = '''
.mode csv
.separator '|'
select 'Loading CSV file','{path}';
.import {path} {table}
'''.format(path=db.path, table=table_name)

        sqlite = subprocess.check_output(["which", "sqlite3"]).strip()

        start = time.clock()
        count = 0

        proc = subprocess.Popen([sqlite,
                                 self.path],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdin=subprocess.PIPE)

        (out, err) = proc.communicate(input=sql)

        if proc.returncode != 0:
            raise Exception("Database load failed: " + str(err))

        diff = time.clock() - start
        return count, diff



class BundleLockContext(object):

    def __init__(self, bundle):
        import traceback
        # from lockfile import FileLock

        self._bundle = bundle
        self._database = self._bundle.database

        tb = traceback.extract_stack()[-4:-3][0]

        global_logger.debug("Using Session Context, from {} in {}:{}".format(tb[2],tb[0],tb[1]))

        self._lock_depth = 0

    def __enter__(self):
        # from sqlalchemy.orm import sessionmaker
        # from ambry.dbexceptions import Locked

        # import pdb;pdb.set_trace()

        # if self._lock_depth == 0:
        #    self._database.lock()

        global_logger.debug("Enter session with depth {}".format(repr(self._lock_depth)))

        self._lock_depth += 1

        return self._database.session

    def __exit__(self, exc_type, exc_val, exc_tb):

        self._lock_depth -= 1

        if exc_type is not None:
            global_logger.debug("Rollback on exception: {}".format(exc_val))
            self._database.session.rollback()
            self._database.close_session()
            # self._database.unlock()
            raise

        else:

            try:
                if self._lock_depth == 0:
                    global_logger.debug(
                        "Commit session {}".format(
                            repr(
                                self._database.session)))
                    self._database.session.commit()
                else:
                    global_logger.debug(
                        "Exit session with depth {}".format(
                            self._lock_depth))

            except Exception as e:
                global_logger.debug('Exception: ' + e.message)
                self._database.session.rollback()
                # self._database.unlock()
                raise

            finally:
                if self._lock_depth == 0:
                    global_logger.debug(
                        "Release session {}".format(
                            repr(
                                self._database.session)))

                    self._database.close_session()
                    # self._database.unlock()
        return True

    def add(self, o):
        return self._bundle._session.add(o)

    def merge(self, o):
        return self._bundle._session.merge(o)


class SqliteBundleDatabase(RelationalBundleDatabaseMixin, SqliteDatabase):

    def __init__(self, bundle, dbname, **kwargs):
        """"""

        RelationalBundleDatabaseMixin._init(self, bundle)
        super(SqliteBundleDatabase, self).__init__(dbname, **kwargs)





    @property
    def has_session(self):
        return self._session is not None

    def query(self, *args, **kwargs):
        """Convenience function for self.connection.execute()"""
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import QueryError

        try:
            return self.connection.execute(*args, **kwargs)
        except OperationalError as e:
            raise QueryError(
                "Error while executing {} in database {} ({}): {}".format(
                    args,
                    self.dsn,
                    type(self),
                    e.message))

    def copy_table_from(self, source_db, table_name):
        """Copy the definition of a table from a soruce database to this one.

        Args:
            table. The name or Id of the table

        """
        from ambry.bundle.schema import Schema

        table = Schema.get_table_from_database(source_db, table_name)

        with self.session_context as s:
            table.session_id = None

            s.merge(table)
            s.commit()

            for column in table.columns:
                column.session_id = None
                s.merge(column)

        return table


class SqliteWarehouseDatabase(SqliteDatabase, SqliteAttachmentMixin):

    pass


class BuildBundleDb(SqliteBundleDatabase):

    """For Bundle databases when they are being built, and the path is computed
    from the build base director."""
    @property
    def path(self):
        return self.bundle.path + self.EXTENSION


def _on_begin_bundle(dbapi_con):

    dbapi_con.execute("BEGIN")

def insert_or_ignore(table, columns):
    return ("""INSERT OR IGNORE INTO {table} ({columns}) VALUES ({values})""".format(
        table=table,
        columns=','.join([c.name for c in columns]),
        values=','.join(['?'] * len(columns))  # @UnusedVariable
        # values=','.join(['?' for c in columns])  # @UnusedVariable
    ))
