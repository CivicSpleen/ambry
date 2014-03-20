"""Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import PartitionBase
from ..identity import PartitionIdentity, PartitionName
from ..database.partition import  PartitionDb


class SqlitePartitionName(PartitionName):
    PATH_EXTENSION = '.db'
    FORMAT = 'db'

class SqlitePartitionIdentity(PartitionIdentity):
    _name_class = SqlitePartitionName


class SqlitePartition(PartitionBase):
    '''Represents a bundle partition, part of the bundle data broken out in
    time, space, or by table. '''

    _id_class = SqlitePartitionIdentity
    _db_class = PartitionDb

    def __init__(self, bundle, record, memory=False, **kwargs):
        
        super(SqlitePartition, self).__init__(bundle, record)
        self.memory  = memory


    @property
    def database(self):
        if self._database is None:
            self._database = PartitionDb(self.bundle, self, base_path=self.path, memory=self.memory)          
        return self._database


    def detach(self, name=None):
        return self.database.detach(name)
     
    def attach(self,id_, name=None):
        return self.database.attach(id_,name)
    
    def create_indexes(self, table=None):

        if not self.database.exists():
            self.create()

        if table is None:
            table = self.get_table()
    
        if isinstance(table,basestring):
            table = self.bundle.schema.table(table)

        for sql in self.bundle.schema.generate_indexes(table):
            print '!!!', sql
            self.database.connection.execute(sql)
              
              
    def drop_indexes(self, table=None):

        if not self.database.exists():
            self.create()

        if table is None:
            table = self.get_table()
        
        if not isinstance(table,basestring):
            table = table.name

        indexes = []

        for row in self.database.query("""SELECT name
            FROM sqlite_master WHERE type='index' AND tbl_name = '{}';""".format(table)):

                if row[0].startswith('sqlite_'):
                    continue

                indexes.append(row[0])

        for index_name in indexes:

                print 'Drop',index_name

                self.database.connection.execute("DROP INDEX {}".format(index_name))
    
    

    
    def create_with_tables(self, tables=None, clean=False):
        '''Create, or re-create,  the partition, possibly copying tables
        from the main bundle
        
        Args:
            tables. String or Array of Strings. Specifies the names of tables to 
            copy from the main bundle. 
            
            clean. If True, delete the database first. Defaults to true. 
        
        '''

        if not tables: 
            raise ValueError("'tables' cannot be empty")

        if not isinstance(tables, (list, set, tuple)):
            tables = [tables]
        else:
            tables = list(tables)

        if clean:
            self.database.delete()

        self.database.create()

        self.add_tables(tables)
        
        
    def add_tables(self,tables):

        for t in tables:
            if not t in self.database.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(t) #@UnusedVariable
                table.create(bind=self.database.engine)       

    def create(self):

        tables = self.data.get('tables',[])

        if tables:
            self.create_with_tables(tables=tables)
        else:
            self.database.create()


    def clean(self):
        '''Delete all of the records in the tables declared for this oartition'''
        
        for table in self.data.get('tables',[]):
            try: self.database.query("DELETE FROM {}".format(table))
            except: pass
        

    def optimal_rows_per_segment(self, size = 100*1024*1024, max=200000):
        '''Calculate how many rows to put into a CSV segment for a target number
        of bytes per file'''
        
        BYTES_PER_CELL = 3.8 # Bytes per num_row * num_col, experimental
        
        # Shoot for about 250M uncompressed, which should compress to about 25M

        table  = self.get_table()
        rows_per_seg = (size / (len(table.columns) * BYTES_PER_CELL) ) 
        
        # Round up to nearest 100K
        
        rows_per_seg = round(rows_per_seg/100000+1) * 100000
        
        if rows_per_seg > max:
            return max
        
        return rows_per_seg
        

    def csvize(self, logger=None, store_library=False, write_header=False, rows_per_seg=None):
        '''Convert this partition to CSV files that are linked to the partition'''
        
        self.table = self.get_table()
        
        if self.record_count:
            self.write_stats()

        if not rows_per_seg:
            rows_per_seg = self.optimal_rows_per_segment()
  
        
        if logger:
            logger.always("Csvize: {} rows per segment".format(rows_per_seg))
        
        ins  =  None
        p = None
        seg = 0
        ident = None
        count = 0
        min_key = max_key = None

        pk = self.table.primary_key.name

        def _store_library(p):
            if store_library:
                if logger:
                    logger.always("Storing {} to Library".format(p.identity.name), now=True)
                    
                dst, _,_ = self.bundle.library.put(p)
                p.database.delete()
                
                if logger:
                    logger.always("Stored at {}".format(dst), now=True)            

        for i,row in enumerate(self.rows):

            if not min_key:
                min_key = row[pk]

            if i % rows_per_seg == 0:
                      
                if p: # Don't do it on the first record. 
                    p.write_stats(min_key, max_key, count)
                    count = 0
                    min_key = row[pk]
                    ins.close()

                    _store_library(p)

                seg += 1
                ident = self.identity
                ident.segment = seg

                p = self.bundle.partitions.find_or_new_csv(**vars(ident.name.as_partialname()))

                ins = p.inserter( write_header=write_header)

                if logger:
                    logger.always("New CSV Segment: {}".format(p.identity.name), now=True)
                
            count += 1
            ins.insert(dict(row))
            max_key = row[pk]
       
            if logger:
                logger("CSVing for {}".format(ident.name))

        # make sure we get the last partition
        if p:
            p.write_stats(min_key, max_key, count)
            ins.close()
            _store_library(p)

    def get_csv_parts(self):
        from ..identity import PartitionNameQuery
        ident = self.identity.clone()   
        ident.format = 'csv'
        ident.segment = PartitionNameQuery.ANY

        return self.bundle.partitions.find_all(PartitionNameQuery(id_=ident))

    def load_csv(self, table=None, parts=None):
        '''Loads the database from a collection of CSV files that have the same identity, 
        except for a format of 'csv' and possible segments. '''

        if not parts:
            parts = self.get_csv_parts()

        
        self.clean()
      
        lr = self.bundle.init_log_rate(100000)
      
        if table is None:
            table = self.table
      
        for p in parts:
            self.bundle.log("Loading CSV partition: {}".format(p.identity.vname))
            self.database.load(p.database, table, logger=lr )
        

    @property
    def rows(self):
        '''Run a select query to return all rows of the primary table. '''
        pk = self.get_table().primary_key.name
        return self.database.query("SELECT * FROM {} ORDER BY {} ".format(self.get_table().name,pk))

    @property
    def pandas(self):
        pk = self.get_table().primary_key.name
        return self.database.select("SELECT * FROM {}".format(self.get_table().name),index_col=pk).pandas


    def query(self,*args, **kwargs):
        """Convience function for self.database.query()"""

        return self.database.query(*args, **kwargs)


    def select(self,sql,*args, **kwargs):
        '''Run a query and return an object that allows the selected rows to be returned
        as a data object in numpy, pandas, petl or other forms'''
        from ..database.selector import RowSelector

        return RowSelector(self, sql,*args, **kwargs)



    def write_stats(self):
        '''Record in the partition entry basic statistics for the partition's
        primary table'''
        t = self.get_table()
        
        if not t:
            return

        if not t.primary_key:
            from ..dbexceptions import ConfigurationError
            raise ConfigurationError("Table {} does not have a primary key; can't compute states".format(t.name))
        
        s = self.database.session
        self.record.count = s.execute("SELECT COUNT(*) FROM {}".format(self.table.name)).scalar()
        self.record.min_key = s.execute("SELECT MIN({}) FROM {}".format(t.primary_key.name,self.table.name)).scalar()
        self.record.max_key = s.execute("SELECT MAX({}) FROM {}".format(t.primary_key.name,self.table.name)).scalar()
        s.commit()
        
        with self.bundle.session as s:
            s.merge(self.record)




    def __repr__(self):
        return "<db partition: {}>".format(self.identity.vname)
