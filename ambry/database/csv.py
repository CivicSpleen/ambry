
"""
Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from __future__ import absolute_import
import unicodecsv

from . import DatabaseInterface
import anydbm
import os
from ..partitions import Partitions

from .inserter import InserterInterface

class ValueInserter(InserterInterface):
    '''Inserts arrays of values into  database table'''
    def __init__(self, path, bundle,  partition, table=None, header=None, delimiter = '|',
                 escapechar='\\', encoding='utf-8', 
                 write_header = False,  buffer_size=2*1024*1024): 
     
        self.table = table
        self.header = header
        self.path = path
        self.buffer_size = buffer_size
        self.delimiter = delimiter
        self.escapechar = escapechar
        self.encoding = encoding
        self.write_header = write_header
        self.bundle = bundle
        self.partition = partition

        if self.header:
            pass
        elif self.table:
            self.header = [c.name for c in self.table.columns]
        else:
            self.table = None
            self.header = None

        self._writer = None
        self._inserter = None
        self._f = None


    def insert(self, values):

        if self._writer is None:
            self._init_writer(values)
      
        try:
            self._inserter(values)
                
        except (KeyboardInterrupt, SystemExit):
            self.close()
            self.delete()
            raise
        except Exception as e:
            self.close()
            self.delete()            
            raise

        return True

     
    def _init_writer(self, row):
        from sqlalchemy.engine.result import RowProxy
        # Four cases:
        #    Write header, or don't
        #    Write list, or dict

        row_is_dict = isinstance(row, dict) or isinstance(row, RowProxy)
        row_is_list = isinstance(row, (list, tuple))


        has_header = self.header is not None

        if not os.path.exists(self.path):
            if not os.path.exists(os.path.dirname(self.path)):
                os.makedirs(os.path.dirname(self.path))

        f = open(self.path, 'wb', buffering=self.buffer_size)
        
        self._f = f

        delimiter = self.delimiter
        
        if row_is_dict and has_header:
            self._writer = unicodecsv.DictWriter(f, self.header, delimiter=delimiter, 
                                                 escapechar=self.escapechar, encoding=self.encoding)
            if self.write_header:
                self._writer.writeheader()
            self._inserter = self._write_dict
            
        elif row_is_dict and not has_header:
            self.header = row.keys()
            self._writer = unicodecsv.DictWriter(f, self.header, delimiter=delimiter, 
                                                 escapechar=self.escapechar, encoding=self.encoding)
            if self.write_header:
                self._writer.writeheader()            
            self._inserter = self._write_dict
            
        elif row_is_list and has_header:
            self._writer = unicodecsv.writer(f, delimiter=delimiter, 
                                             escapechar=self.escapechar, encoding=self.encoding)
            if self.write_header:
                self._writer.writerow(self.header)
            self._inserter = self._write_list
            
        elif row_is_list and not has_header:
            self._writer = unicodecsv.writer(f, delimiter=delimiter, 
                                             escapechar=self.escapechar, encoding=self.encoding)
            self._inserter = self._write_list

        else:
            raise Exception("Unexpected case for type {}".format(type(row)))


    def _write_list(self, row):
        self._writer.writerow(row)
     
    def _write_dict(self, row):
        self._writer.writerow(row)
     
    def close(self):
        if self._f and not self._f.closed:
            self._f.flush()
            self._f.close()

    def delete(self):
        import os
        if os.path.exists(self.path):
            os.remove(self.path)
     
    @property
    def linewriter(self):
        '''Like writer, but does not write a header. '''
        if self._writer is None:
            import csv
            self.close()
            
            if self.exists:
                mode = 'a+'
            else:
                mode = 'w'

            self.file = open(self.path, mode, buffering=self.buffer_size)
            self._writer = csv.writer(self.file)

        return self._writer
            
    def __enter__(self):

        self.partition.set_state(Partitions.STATE.BUILDING)
        return self
    
    def __exit__(self, type_, value, traceback):

        if type_ is not None:
            self.bundle.error("Got Exception: " + str(value))
            self.partition.set_state(Partitions.STATE.ERROR)
            return False

        self.partition.set_state(Partitions.STATE.BUILT)

        self.close()

from . import DatabaseInterface

class CsvDb(DatabaseInterface): 
     
    EXTENSION = '.csv'
     
    def __init__(self, bundle, partition, base_path, **kwargs):
        ''''''   
        
        self.bundle = bundle
        self.partition = partition

      
        self.delimiter = '|'
    
      
    @property 
    def path(self):
        return self.partition.path+self.EXTENSION

    @property
    def md5(self):
        from ambry.util import md5_for_file
        return md5_for_file(self.path)

    def exists(self):
        import os
        return os.path.exists(self.path)
        
    def is_empty(self):
        
        if not self.exists():
            return True
        
        import os
        statinfo = os.stat(self.path)
        
        if statinfo.st_size == 0:
            return True
        else:
            return False
        
    def create(self):
        pass # Created in the inserter
        
    def delete(self):
        import os
        if os.path.exists(self.path):
            os.remove(self.path)
        
    def inserter(self, header=None, skip_header = False, **kwargs):
        
        if not skip_header and header is None and self.partition.table is not None:
            header = [c.name for c in self.partition.table.columns]

        return ValueInserter(self.path,  self.bundle, self.partition,  header=header, **kwargs)
        
    def reader(self,  encoding='utf-8', *args, **kwargs):

        f = open(self.path,'rb')

        return unicodecsv.reader(f, *args, delimiter=self.delimiter, encoding='utf-8', **kwargs)
        
    def dict_reader(self, encoding='utf-8', *args, **kwargs):

        f = open(self.path,'rb')

        return unicodecsv.DictReader(f,*args, delimiter=self.delimiter, encoding='utf-8', **kwargs)
        
    def close(self):
        pass

