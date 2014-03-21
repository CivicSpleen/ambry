'''
Created on Sep 7, 2013

@author: eric
'''
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
from ambry.util import get_logger
import logging

logger = get_logger(__name__)
#logger.setLevel(logging.DEBUG)

class InserterInterface(object):
    
    def __enter__(self): raise NotImplemented()
    
    def __exit__(self, type_, value, traceback): raise NotImplemented()
    
    def insert(self, row, **kwargs): raise NotImplemented()
    
    def close(self): raise NotImplemented()


class UpdaterInterface(object):
    
    def __enter__(self): raise NotImplemented()
    
    def __exit__(self, type_, value, traceback): raise NotImplemented()
    
    def update(self, row): raise NotImplemented()
    
    def close(self): raise NotImplemented()


class SegmentInserterFactory(object):
    
    def next_inserter(self, segment): 
        raise NotImplemented()


class SegmentedInserter(InserterInterface):

    def __init__(self, segment_size=100000, segment_factory = None):
        pass
    
        self.segment = 1
        self.inserter = None
        self.count = 0
        self.segment_size = segment_size
        self.factory = segment_factory

        self.inserter = self.factory.next_inserter(self.segment)
       
        self.inserter.__enter__()
    
    def __enter__(self): 

        return self
            
    def __exit__(self, type_, value, traceback):
        self.inserter.__exit__(type_, value, traceback)
        return self
    
    def insert(self, row, **kwargs):
        
        self.count += 1
        
        if self.count > self.segment_size:
            self.segment += 1
            self.inserter.__exit__(None,None,None)
            self.inserter = self.factory.next_inserter(self.segment)
            self.inserter.__enter__()
            
            self.count = 0

        return self.inserter.insert(row)
        
    
    def close(self):
        self.inserter.close()


class ValueWriter(InserterInterface):
    '''Inserts arrays of values into  database table'''
    def __init__(self,  db, bundle,  cache_size=50000, text_factory = None, replace=False):
        import string 
        self.cache = []
        
        self.bundle = bundle
        self.db = db
        self.session = self.db.session
        self.session.commit()
        self.session.flush()

        self.cache_size = cache_size
        self.statement = None
        
        if text_factory:
            self.db.engine.raw_connection().connection.text_factory = text_factory

    def __enter__(self):
        from ..partitions import Partitions
        self.db.partition.set_state(Partitions.STATE.BUILDING)
        return self
        
    def rollback(self):
        from ..partitions import Partitions
        logger.debug("rollback {}".format(repr(self.session)))
        self.session.rollback()
        self.db.partition.set_state(Partitions.STATE.ERROR)
    
    def commit_end(self):
        from ..partitions import Partitions
        logger.debug("commit end {}".format(repr(self.session)))
        self.session.commit()
        self.db.partition.set_state(Partitions.STATE.BUILT)
        
    def commit_continue(self):
        from ..partitions import Partitions
        logger.debug("commit continue {}".format(repr(self.session)))
        self.session.commit()
        self.db.partition.set_state(Partitions.STATE.BUILDING)
 
    def close(self):

        if len(self.cache) > 0 :       
            try:
                self.session.execute(self.statement, self.cache)
                self.commit_end()
                self.cache = []
            except (KeyboardInterrupt, SystemExit):
                self.rollback()
                raise
            except Exception as e:
                if self.bundle:
                    self.bundle.error("Exception during ValueWriter.insert: "+str(e))
                self.rollback()
                raise
        else:
            self.commit_end()
                    
    def __exit__(self, type_, value, traceback):
    
        if type_ is not None:
            try:
                self.bundle.error("Got exception while exiting inserter context: "+str(value))
            except:
                print "ERROR: Got Exception {}: {}".format(type_, str(value))
                self.rollback()
            return False

        self.close()
           
                
        return self
        
 
class CodeCastErrorHandler(object):
    '''Used by the Value Inserter to handle errors in casting
    data types. This version will create code table entries
    for any values that can't be cast.  '''
    
    def __init__(self, inserter):
        from collections import defaultdict
        self.codes = defaultdict(set)
        self.inserter = inserter
    
    def cast_error (self, row,  cast_errors):
        '''For each cast error, save the key and value in a set, 
        for later conversion to a code partition '''
        for k,v in cast_errors.items():
            self.inserter.bundle.schema.add_code_table(self.inserter.table.name, k) # idempotent
            self.codes[k].add(v)
            
    def finish(self):
        '''For each of the code sets generated during casting errors, create
        a new partition that holds all of the codes. '''
        for col_name, codes in self.codes.items():
            p = self.inserter.bundle.partitions.find_or_new(
                table=self.inserter.table.name+'_'+col_name+'_codes')
            
            with p.inserter() as ins:
                for code in codes:
                    ins.insert({'code':code})


class ValueInserter(ValueWriter):
    '''Inserts arrays of values into  database table'''
    def __init__(self, db,  bundle, table, 
                 orm_table = None,
                 cast_error_handler = None,
                 cache_size=50000, text_factory = None, 
                 replace=False,  skip_none=True, update_size = True):

        super(ValueInserter, self).__init__(db, bundle,  cache_size=cache_size, text_factory = text_factory)  
   
        if table is None and bundle is None:
            raise ValueError("Must define either table or bundle")
   
        self.table = table

        # The   _db_orm_table is added to the Sqlalchemy metadata in 
        # RelationalDatabase.table() 
        if hasattr(self.table,'_db_orm_table'):
            self.orm_table = self.table._db_orm_table
            self.caster = self.orm_table.caster
        else:
            self.orm_table = self.bundle.schema.table(table.name)
            self.caster = self.bundle.schema.caster(table.name)


        self.null_row = self.orm_table.null_dict

        self.cast_error_handler = cast_error_handler(self) if cast_error_handler else None

        self.header = [c.name for c in self.orm_table.columns]

        # Int is included b/c long integer values get a type of integer64
        self.sizable_fields = [c.name for c in self.orm_table.columns if c.type_is_text() or c.datatype == c.DATATYPE_INTEGER ]

        self._max_lengths = [ 0 for x in self.sizable_fields ]
   
        self.statement = self.table.insert()
 
        self.skip_none = skip_none

        self.update_size = update_size


        if replace:
            self.statement = self.statement.prefix_with('OR REPLACE')

    def insert(self, values):
        from sqlalchemy.engine.result import RowProxy
        
        if isinstance(values, RowProxy):
            values = dict(values)

        code_dict = None

        try:
            cast_errors =  None

            if isinstance(values, dict):

                if self.caster:
                    d, cast_errors = self.caster(values)

                else:
                    d = dict((k.lower(), v) for k,v in values.items())

                if self.skip_none:

                    d = { k: d[k] if k in d and d[k] is not None else v for k,v in self.null_row.items() }

            else:
                
                if self.caster:
                    d, cast_errors = self.caster(values)
                else:
                    d = values

                d  = dict(zip(self.header, d))
                
                if self.skip_none:
                    d = { k: d[k] if k in d and d[k] is not None else v for k,v in self.null_row.items() }
                
            if self.update_size:
                for i,col_name in enumerate(self.sizable_fields):
                    try:
                        self._max_lengths[i] = max(len(str(d[col_name])) if d[col_name] else 0, self._max_lengths[i])
                    except UnicodeEncodeError:
                        # Unicode is a PITA
                        pass

            self.cache.append(d)
         
            if len(self.cache) >= self.cache_size: 
                self.session.execute(self.statement, self.cache)
                self.cache = []
                self.commit_continue()


            if cast_errors and self.cast_error_handler:
                self.cast_error_handler.cast_error(values,cast_errors )

            return cast_errors

        except (KeyboardInterrupt, SystemExit):
            if self.bundle:
                self.bundle.log("Processing keyboard interrupt or system exist")
            else:
                print "Processing keyboard interrupt or system exist" 
            self.rollback()
            self.cache = []
            raise
        except Exception as e:
            if self.bundle:
                self.bundle.error("Insert exception: {}".format(e))
                raise
            else:
                print "ERROR: Exception during ValueInserter.insert: {}".format(e)
            self.rollback()
            self.cache = []
            raise

        return True
   
    @property
    def max_lengths(self):
        return dict(zip(self.sizable_fields, self._max_lengths))
   
    def __exit__(self, type_, value, traceback):
        
        super(ValueInserter, self).__exit__(type_, value, traceback)

        if self.update_size and self.bundle:
            self.bundle.schema.update_lengths(self.table.name, self.max_lengths)
   
        if self.cast_error_handler:
            self.cast_error_handler.finish()
   
   
class ValueUpdater(ValueWriter, UpdaterInterface):
    '''Updates arrays of values into  database table'''
    def __init__(self,  db, bundle, table,  cache_size=50000, text_factory = None): 
        
        from sqlalchemy.sql.expression import bindparam, and_
        super(ValueUpdater, self).__init__(db, bundle,  cache_size=50000, text_factory = text_factory)
    
        self.table = table
        self.statement = self.table.update()
     
        wheres = []
        for primary_key in table.primary_key:
            wheres.append(primary_key == bindparam('_'+primary_key.name))
            
        if len(wheres) == 1:
            self.statement = self.statement.where(wheres[0])
        else:
            self.statement = self.statement.where(and_(wheres))
       
        self.values = None
       

    def update(self, values):
        from sqlalchemy.sql.expression import bindparam
        
        if not self.values:
            names = values.keys()
            
            binds = {}
            for col_name in names:
                if not col_name.startswith("_"):
                    raise ValueError("Columns names must start with _ for use in updater")
                
                column = self.table.c[col_name[1:]]
                binds[column.name] = bindparam(col_name)
                
                self.statement = self.statement.values(**binds)
       
        try:
            if isinstance(values, dict):
                d = values
            else:
                d  = dict(zip(self.header, values))
         
            self.cache.append(d)
         
            if len(self.cache) >= self.cache_size:
                
                self.session.execute(self.statement, self.cache)
                self.cache = []
                
        except (KeyboardInterrupt, SystemExit):
            self.rollback()
            self.cache = []
            raise
        except Exception as e:
            self.bundle.error("Exception during ValueUpdater.insert: "+str(e))
            self.rollback()
            self.cache = []
            raise e

        return True    
