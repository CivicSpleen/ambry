"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from .sqlite import SqliteDatabase, SqliteAttachmentMixin #@UnresolvedImport
from .relational import RelationalPartitionDatabaseMixin, RelationalDatabase #@UnresolvedImport
from inserter import ValueInserter, ValueUpdater

class PartitionDb(SqliteDatabase, RelationalPartitionDatabaseMixin, SqliteAttachmentMixin):
    '''a database for a partition file. Partition databases don't have a full schema
    and can load tables as they are referenced, by copying them from the prototype. '''

    def __init__(self, bundle, partition, base_path, memory = False, **kwargs):
        '''''' 

        RelationalPartitionDatabaseMixin._init(self,bundle,partition)
        self.memory = memory
    
        super(PartitionDb, self).__init__(base_path, memory=self.memory, **kwargs)  

        self._session = None

        assert partition.identity.extension() == self.EXTENSION, (
            "Identity extension '{}' not same as db extension '{}' for database {}".format(
            partition.identity.extension(), self.EXTENSION, type(self)
        ))

    def query(self,*args, **kwargs):
        """Convenience function for self.connection.execute()"""
        from sqlalchemy.exc import OperationalError
        from ..dbexceptions import QueryError
        
        if isinstance(args[0], basestring):
            fd = { x:x for x in self._attachments }
        
            args = (args[0].format(**fd),) + args[1:]
            
        try:
            return self.connection.execute(*args, **kwargs)
        except OperationalError as e:
            raise QueryError("Error while executing {} in database {} ({}): {}".format(args, self.dsn, type(self), e.message))
        

    def inserter(self, table_or_name=None,**kwargs):

        if not self.exists():

            raise Exception("Database doesn't exist yet: '{}'".format(self.dsn))

        if table_or_name is None and self.partition.table is not None:
            table_or_name = self.partition.get_table()
      
        if isinstance(table_or_name, basestring):

            table_name = table_or_name
            
            if not table_name in self.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(table_name) #@UnusedVariable
                table.create(bind=self.engine)
                
                if not table_name in self.inspector.get_table_names():
                    raise Exception("Don't have table "+table_name)
                
            table = self.table(table_name)
            
        else:
            table = self.table(table_or_name.name)

        
        return ValueInserter(self, self.bundle, table ,  **kwargs)
        
    def updater(self, table_or_name=None,**kwargs):
      
        if table_or_name is None and self.partition.table is not None:
            table_or_name = self.partition.table
      
        if isinstance(table_or_name, basestring):
            table_name = table_or_name
            if not table_name in self.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(table_name) #@UnusedVariable
                table.create(bind=self.engine)
                
                if not table_name in self.inspector.get_table_names():
                    raise Exception("Don't have table "+table_name)
            table = self.table(table_name)
            
        else:
            table = self.table(table_or_name.name)
            
        return ValueUpdater(self, self.bundle, table , **kwargs)


    def _on_create_connection(self, connection):
        '''Called from get_connection() to update the database'''
        super(PartitionDb, self)._on_create_connection(connection)

        _on_connect_partition(connection, None)

    def _on_create_engine(self, engine):
        from sqlalchemy import event

        super(PartitionDb, self)._on_create_engine(engine)

        # This looks like it should be connected to the listener, but it causes
        # I/O errors, so it is in _on_create_connection
        #event.listen(self._engine, 'connect', _on_connect_partition)


    def create(self):
        from ambry.orm import Dataset

        '''Like the create() for the bundle, but this one also copies
        the dataset and makes and entry for the partition '''
        

        self.require_path()
        
        SqliteDatabase._create(self) # Creates the database file
        
        if RelationalDatabase._create(self):
            self.post_create()


    # DEPRECATED! Should use the session_context instead
    @property
    def session(self):
        '''Return a SqlAlchemy session'''
        from sqlalchemy.orm import sessionmaker
        if not self._session:
            Session = sessionmaker(bind=self.engine,autocommit=False)
            self._session =  Session()
            
        return self._session

def _on_connect_partition(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''
    from sqlite import _on_connect_bundle as ocb

    ocb(dbapi_con, con_record)



    #dbapi_con.enable_load_extension(True)
