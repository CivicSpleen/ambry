
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from __future__ import absolute_import
from .relational import RelationalBundleDatabaseMixin, RelationalDatabase #@UnresolvedImport
import os
from ambry.util import get_logger

import logging

logger = get_logger(__name__)
#logger.setLevel(logging.DEBUG)
logger.debug("Init database logger")

class SqliteAttachmentMixin(object):
    
    
    def attach(self,id_, name=None, conn=None):
        """Attach another sqlite database to this one
        
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
    
        if isinstance(id_,basestring):
            #  Strings are path names
            path = id_
        elif isinstance(id_, Identity):
            path = id_.path
        elif isinstance(id_,PartitionDb):
            path = id_.path
        elif isinstance(id_,PartitionInterface):
            path = id_.database.path
        elif isinstance(id_,Bundle):
            path = id_.database.path
        else:
            raise Exception("Can't attach: Don't understand id_: {}".format(repr(id_)))
        
        if name is None:
            import random, string
            name =  ''.join(random.choice(string.letters) for i in xrange(10)) #@UnusedVariable
        
        q = """ATTACH DATABASE '{}' AS '{}' """.format(path, name)

        if not conn:
            conn = self.connection

        conn.execute(q)
           
        self._last_attach_name = name


        self._attachments.add(name)
        
        return name
        
    def detach(self, name=None):
        """Detach databases
        
        Args:
            name. Name of database to detach. If None, detatch all
            
        
        """
    
        if name is None:
            name = self._last_attach_name
    
        self.connection.execute("""DETACH DATABASE {} """.format(name))
    
        self._attachments.remove(name)
    
    
    def copy_from_attached(self, table, columns=None, name=None, 
                           on_conflict= 'ABORT', where=None, conn=None):
        """ Copy from this database to an attached database
        
        Args:
            map_. a dict of k:v pairs for the values in this database to
            copy to the remote database. If None, copy all values
        
            name. The attach name of the other datbase, from self.attach()
        
            on_conflict. How conflicts should be handled
            
            where. An additional where clause for the copy. 
            
        """
        
        if name is None:
            name = self._last_attach_name
        
        f = {'db':name, 'on_conflict': on_conflict, 'from_columns':'*', 'to_columns':''}
        
        if isinstance(table,basestring):
            # Copy all fields between tables with the same name
            f['from_table']  = table
            f['to_table'] = table
    
        elif isinstance(table, tuple):
            # Copy all fields between two tables with different names
            f['from_table'] = table[0]
            f['to_table'] = table[1]
        else:
            raise Exception("Unknown table type "+str(type(table)))

        if columns is None:
            pass
        elif isinstance(columns, dict):
            f['from_columns'] = ','.join([ k for k,v in columns.items() ])
            f['to_columns'] =  '('+','.join([ v for k,v in columns.items() ])+')'
            
        q = """INSERT OR {on_conflict} INTO {to_table} {to_columns} 
               SELECT {from_columns} FROM {db}.{from_table}""".format(**f)
    
        if where is not None:
            q = q + " " + where.format(**f)

        if conn:
            conn.execute(q)
        else:
            with self.engine.begin() as conn:
                conn.execute(q)


class SqliteDatabase(RelationalDatabase):

    EXTENSION = '.db'
    SCHEMA_VERSION = 16

    def __init__(self, dbname, memory = False,  **kwargs):   
        ''' '''
    
        # For database bundles, where we have to pass in the whole file path
        if memory:
            base_path = ':memory:'
        else:
            base_path, ext = os.path.splitext(dbname)
            
            if ext and ext != self.EXTENSION:
                raise Exception("Bad extension to file {}: {}: {}".format(dbname, base_path, ext))
            
            self.base_path = base_path

        self._last_attach_name = None
        self._attachments = set()

        # DB-API is needed to issue INSERT OR REPLACE type inserts. 
        self._dbapi_cursor = None
        self._dbapi_connection = None
        self.memory = memory

        if not 'driver' in kwargs:
            kwargs['driver'] = 'sqlite'

        super(SqliteDatabase, self).__init__(dbname=self.path,   **kwargs)
        
    @property 
    def path(self):
        if self.memory:
            return ':memory:'
        else:
            return self.base_path+self.EXTENSION

    @property
    def md5(self):
        from ambry.util import md5_for_file
        return md5_for_file(self.path)

    @property
    def lock_path(self):
        return self.base_path

    def require_path(self):
        if not self.memory:
            if not os.path.exists(os.path.dirname(self.base_path)):
                os.makedirs(os.path.dirname(self.base_path))
            
    
    @property
    def version(self):
        v =  self.connection.execute('PRAGMA user_version').fetchone()[0]
    
        try:
            return int(v)
        except:
            return 0
    


    def _on_connect(self, conn):
        '''Called from engine() to update the database'''
        _on_connect_update_sqlite_schema(conn)
        _on_connect_bundle(conn)


    @property
    def connection(self):
        return self.get_connection()

    def get_connection(self, check_exists=True):
        '''Return an SqlAlchemy connection'''
        if not self._connection:
            
            if not os.path.exists(self.path) and check_exists:
                from ..dbexceptions import DatabaseMissingError
                raise DatabaseMissingError("Trying to make a connection to a sqlite database "+
                                "that does not exist. check_exists={} path={}"
                                .format(check_exists, self.path))

            try:
    
                self._connection = self.engine.connect()
            except Exception as e:
                self.error("Failed to open: '{}': {} ".format(self.path, e))
                raise
            
        return self._connection

    @property
    def unmanaged_session(self):
        
        def abort_flush():
            from ambry.dbexceptions import ConflictError
            raise ConflictError('Unmanaged sessions are read-only. Use a managed session to write to the database')
        
        if not self._unmanaged_session:
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=self.engine,autocommit=False, autoflush=False)
            self._unmanaged_session =  Session()
            
            self._unmanaged_session.flush = abort_flush # Monkeypatch to make read-only

        return self._unmanaged_session

    def _create(self):
        """Need to ensure the database exists before calling for the connection, but the
        connection expects the database to exist first, so we create it here. """
        
        from sqlalchemy import create_engine  
        
        engine = create_engine(self.dsn, echo=False)
        connection = engine.connect()

        connection.execute("PRAGMA user_version = {}".format(self.SCHEMA_VERSION))

    MIN_NUMBER_OF_TABLES = 1
    def is_empty(self):
        
        if not os.path.exists(self.path):
            return True
        
        if  self.version >= 12:
            if not 'config' in self.inspector.get_table_names():
                return True
            else:
                return False
        else:

            tables = self.inspector.get_table_names()

            if tables and len(tables) < self.MIN_NUMBER_OF_TABLES:
                return True
            else:
                return False
            

    @property
    def dbapi_connection(self):
        '''Return an DB_API connection'''
        import sqlite3
        if not self._dbapi_connection:
            self._dbapi_connection = sqlite3.connect(self.path)
            
        return self._dbapi_connection

    @property
    def dbapi_cursor(self):
        '''Return an DB_API cursor'''
        if not self._dbapi_cursor:
        
            self._dbapi_cursor = self.dbapi_connection.cursor()
            
        return self._dbapi_cursor
    
    def dbapi_close(self):
        '''Close both the cursor and the connection'''
        if  self._dbapi_cursor:
            self._dbapi_cursor.close()
            self._dbapi_cursor = None
            
        if  self._dbapi_connection:
            self._dbapi_connection.close()
            self._dbapi_connection = None    

        
    def delete(self):
        
        try :
            os.remove(self.path)
        except:
            pass
        
    def clean(self):
        '''Remove all files generated by the build process'''
        os.remove(self.path)


    def load(self,a, table=None, encoding='utf-8', caster = None, logger=None):
        ''' Load the database from a CSV file '''
        
        #return self.load_insert(a,table, encoding=encoding, caster=caster, logger=logger)
        return self.load_shell(a,table, encoding=encoding, caster=caster, logger=logger)

    def load_insert(self,a, table=None, encoding='utf-8', caster=None, logger=None):
        from ..partition import PartitionInterface
        from ..database.csv import CsvDb
        from ..dbexceptions import ConfigurationError
        import time
        
        if isinstance(a,PartitionInterface):
            db = a.database
        elif isinstance(a,CsvDb):
            db = a
        else:
            raise ConfigurationError("Can't use this type: {}".format(type(a)))

    
        start = time.clock()
        count = 0
        with self.inserter(table,  caster=caster) as ins:
            for row in db.reader(encoding=encoding):
                count+=1
             
                if logger:
                    logger("Load row {}:".format(count))
             
                ins.insert(row)
        
        diff = time.clock() - start
        return count, diff
        
    def load_shell(self,a, table, encoding='utf-8', caster=None, logger=None):
        from ..partition import PartitionInterface
        from ..database.csv import CsvDb
        from ..dbexceptions import ConfigurationError
        import time
        import subprocess, uuid
        from ..util import temp_file_name
        import os
        
        if isinstance(a,PartitionInterface):
            db = a.database
        elif isinstance(a,CsvDb):
            db = a
        else:
            raise ConfigurationError("Can't use this type: {}".format(type(a)))
        

        try: table_name = table.name
        except AttributeError: table_name = table
        
        sql_file = temp_file_name()
        
        sql = '''
.mode csv
.separator '|'
select 'Loading CSV file','{path}';
.import {path} {table}
'''.format(path=db.path, table=table_name)

        sqlite = subprocess.check_output(["which", "sqlite3"]).strip()

        start = time.clock()
        count = 0

        proc = subprocess.Popen([sqlite,  self.path], 
                                stdout=subprocess.PIPE,  stderr=subprocess.PIPE, stdin=subprocess.PIPE)

        (out, err) = proc.communicate(input=sql)
        
        if proc.returncode != 0:
            raise Exception("Database load failed: "+str(err))

        diff = time.clock() - start
        return count, diff


class BundleLockContext(object):
    
    def __init__( self, bundle):
        import traceback
        from lockfile import FileLock

        self._bundle = bundle
        
        self._lock_path = self._bundle.path

        self._lock = FileLock(self._lock_path)

        if not hasattr(self._bundle,'_lock_depth' ):
            self._bundle._lock_depth  = 0

        tb = traceback.extract_stack()[-4:-3][0]

        logger.debug("Using Lock Context, from {} in {}:{}".format(tb[2], tb[0], tb[1]))
        
            
    def __enter__( self ):
        from sqlalchemy.orm import sessionmaker
        from ambry.dbexceptions import Locked
        import lockfile
        
        if self._bundle._session:
            self._session = self._bundle._session
            #logger.debug("Failing to acquire lock on {}, bundle already has session".format(self._bundle.dsn))
            #raise Locked("Bundle already has a session, {}".format(repr(self._bundle._session)))
        else:
            Session = sessionmaker(bind=self._bundle.engine,autocommit=False)
            self._session =  Session()

        logger.debug("Acquiring lock on {}".format(self._bundle.dsn))
        
        while True:
            try:
                self._lock.acquire(5)
                    
                self._bundle._lock_depth += 1
                logger.debug("Acquired lock on {}. Depth = {}".format(self._bundle.dsn, self._bundle._lock_depth))
                break
            except lockfile.LockTimeout as e:
                logger.warn(e.message)

        self._bundle._session = self._session
        return self._session
    
    def __exit__( self, exc_type, exc_val, exc_tb ):

        if  exc_type is not None:
            logger.debug("Release lock and rollback on exception: {}".format(exc_val))
            self._session.rollback()
            self._bundle._lock_depth -= 1
            self._lock.release()
            self._bundle._session.close()
            self._bundle._session = None
            raise
            return False
        else:
            logger.debug("Release lock and commit session {}. Depth = {}".format(repr(self._session), self._bundle._lock_depth))
            try:
                self._session.commit()
            except:
                self._session.rollback()
                raise
            finally:
                self._bundle._lock_depth -= 1
                if self._bundle._lock_depth == 0:
                    logger.debug("Released lock and commit session {}".format(repr(self._session)))
                    self._lock.release()
                    self._bundle._session.close()
                    self._bundle._session = None
            
            return True
            
    def add(self,o):
        self._bundle._session.add(o) 
            

class SqliteBundleDatabase(RelationalBundleDatabaseMixin,SqliteDatabase):

    def __init__(self, bundle, dbname, **kwargs):   
        '''
        '''

        RelationalBundleDatabaseMixin._init(self, bundle)
        super(SqliteBundleDatabase, self).__init__(dbname,  **kwargs)

        self._session = None # This is controlled by the BundleLockContext


    def create(self):

        self.require_path()
  
        SqliteDatabase._create(self) # Creates the database file
        
        if RelationalDatabase._create(self):
            
            RelationalBundleDatabaseMixin._create(self)

            self.post_create()
          

        
    @property
    def session(self):
        from ..dbexceptions import  NoLock
        
        if not self._session:
            return self.unmanaged_session

        logger.debug("    Using a managed session {} for {}".format(repr(self._session),self.dsn))
        return self._session
        
    
        
    @property
    def has_session(self):
        return self._session is not None

    def query(self,*args, **kwargs):
        """Convenience function for self.connection.execute()"""
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import QueryError
        
        try:
            return self.connection.execute(*args, **kwargs)
        except OperationalError as e:
            raise QueryError("Error while executing {} in database {} ({}): {}".format(args, self.dsn, type(self), e.message))

                
    @property
    def lock(self):
        return BundleLockContext(self)
        
            
    def copy_table_from(self, source_db, table_name):
        '''Copy the definition of a table from a soruce database to this one
        
        Args:
            table. The name or Id of the table
        
        '''
        from ambry.schema import Schema
        
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


class SqliteMemoryDatabase(SqliteDatabase, SqliteAttachmentMixin):

    def __init__(self):   
        '''
        '''

        super(SqliteMemoryDatabase,self).__init__( None, memory = True)  

    def query(self,*args, **kwargs):
        """Convience function for self.connection.execute()"""
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import QueryError

        if isinstance(args[0], basestring):
            fd = { x:x for x in self._attachments }
            args = list(args)
            first = args.pop(0)
            args = [first.format(**fd),] + args
            
        try:
            return self.connection.execute(*args, **kwargs)
        except OperationalError as e:
            raise QueryError("Error while executing {} in database {} ({}): {}".format(args, self.dsn, type(self), e.message))


def _on_connect_bundle(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created
    
    Bundles have different parameters because they are more likely to be accessed concurrently. 
    '''
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('PRAGMA foreign_keys = ON')


def _on_connect_update_sqlite_schema(conn):
    '''Perform on-the-fly schema updates based on the user version'''

    version = conn.execute('PRAGMA user_version').fetchone()[0]

    if version:
        version = int(version)

    
    if version > 10: # Some files have version of 0 because the version was not set.

        if  version < 14:

            raise Exception("There should not be any files of less than version 14 in existence. Got: {}".format(version))


        if  version < 15:

            try: conn.execute('ALTER TABLE datasets ADD COLUMN d_cache_key VARCHAR(200);')
            except: pass

            try: conn.execute('ALTER TABLE partitions ADD COLUMN p_cache_key VARCHAR(200);')
            except: pass


        if version < 16:

            try:
                conn.execute('ALTER TABLE tables ADD COLUMN t_universe VARCHAR(200);')
            except:
                pass


    conn.execute('PRAGMA user_version = {}'.format(SqliteDatabase.SCHEMA_VERSION))


class BuildBundleDb(SqliteBundleDatabase):
    '''For Bundle databases when they are being built, and the path is computed from 
    the build base director'''
    @property 
    def path(self):
        return self.bundle.path + self.EXTENSION

 
def insert_or_ignore(table, columns):
    return  ("""INSERT OR IGNORE INTO {table} ({columns}) VALUES ({values})"""
                            .format(
                                 table=table,
                                 columns =','.join([c.name for c in columns ]),
                                 values = ','.join(['?' for c in columns]) #@UnusedVariable
                            )
                         )
    


  
