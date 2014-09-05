"""The schema sub-object provides acessors to the schema for a bundle. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from ambry.dbexceptions import ConfigurationError
from ambry.orm import Column
from collections import OrderedDict, defaultdict

def _clean_flag( in_flag):
    
    if in_flag is None or in_flag == '0':
        return False

    return bool(in_flag)

def _clean_int(i):
    
    if isinstance(i, int):
        return i
    elif isinstance(i, basestring):
        if len(i) == 0:
            return None
        
        return int(i.strip())
    elif i is None:
        return None
        raise ValueError("Input must be convertable to an int. got:  ".str(i)) 

class Schema(object):
    """Represents the table and column definitions for a bundle
    """
    def __init__(self, bundle):
        from bundle import  Bundle
        from collections import defaultdict
        self.bundle = bundle # COuld also be a partition
        
        # the value for a Partition will be a PartitionNumber, and
        # for the schema, we want the dataset number
        if not isinstance(self.bundle, Bundle):
            raise Exception("Can only construct schema on a Bundle")

        self.d_id=self.bundle.identity.id_
 
        self._seen_tables = {}
        self.table_sequence = None
        self.max_col_id = {}

        # Cache for references to code tables. 
        self._code_table_cache = None
        
        # Flag to indicate that new code tables were added, so the
        # build should be re-run
        self.new_code_tables = False

    @property
    def dataset(self,):
        '''Initialize the identity, creating a dataset record, 
        from the bundle.yaml file'''
        
        from ambry.orm import Dataset

        return (self.bundle.database.session.query(Dataset).one())

    def clean(self):
        '''Delete all tables and columns. 
        WARNING! This will also delete partitions, since partitions can depend on tables
        '''
        
        from ambry.orm import Table, Column, Partition
        
        self._seen_tables = {}
        self.table_sequence = None
        self.max_col_id = {}

        with self.bundle.session as s:
            s.query(Partition).delete()        
            s.query(Column).delete() 
            s.query(Table).delete()       
            
    @property
    def tables(self):
        '''Return a list of tables for this bundle'''
        from ambry.orm import Table

        from ambry.orm import Table
        
        q = (self.bundle.database.session.query(Table).filter(Table.d_id==self.d_id))

        return q.all()

    @classmethod
    def get_table_from_database(cls, db, name_or_id, session=None, d_vid=None):
        '''Return the orm.Table record from the bundle schema '''

        from ambry.orm import Table
        
        import sqlalchemy.orm.exc
        from sqlalchemy.sql import or_, and_
        
        if not name_or_id:
            raise ValueError("Got an invalid argument: {}".format(name_or_id))

        try: 
            if d_vid:
                return (session.query(Table).filter(
                         and_(Table.d_vid ==  d_vid,   
                         or_(Table.vid==name_or_id,
                             Table.id_==name_or_id,
                             Table.name==name_or_id))
                        ).one())
                
            else:
    
                return (session.query(Table).filter(
                         or_(Table.vid==name_or_id,
                             Table.id_==name_or_id,
                             Table.name==name_or_id)
                        ).one())
                
        except sqlalchemy.orm.exc.NoResultFound as e:
            raise sqlalchemy.orm.exc.NoResultFound("No table for name_or_id: {}".format(name_or_id))

    def table(self, name_or_id, session = None):
        '''Return an orm.Table object, from either the id or name. This is the cleaa method version
        of get_table_from_database'''

        if session is None:
            session = self.bundle.database.session


        return Schema.get_table_from_database(self.bundle.database, name_or_id,
                                              session = session,
                                              d_vid = self.bundle.identity.vid)

    def column(self, table, column_name):

        for c in table:
            if c.name == column_name:
                return c

        return None


    def add_table(self, name,  **kwargs):
        '''Add a table to the schema, or update it it already exists.

        If updating, will only update data.
        '''
        from orm import Table
        from sqlalchemy.orm.exc import NoResultFound
        from identity import TableNumber, ObjectNumber
           
        if not self.table_sequence:
            self.table_sequence = len(self.tables)+1
           
        name = Table.mangle_name(name)

        in_data = kwargs.get('data',{})

        col_data = { k.replace('d_','',1): v for k,v in kwargs.items() if k.startswith('d_') }

        try:
            row = self.table(name)
        except NoResultFound as e:
            row = None

        if row:
            extant = True
            row.data=dict(row.data.items() + col_data.items() + in_data.items())
            self.col_sequence = len(row.columns)

        else:
            extant = False
            row = Table(self.dataset,
                        name=name,
                        sequence_id=self.table_sequence,
                        data=dict(col_data.items() + in_data.items()))

            self.table_sequence += 1
            self.col_sequence = 1
            self.max_col_id = {}


        for key, value in kwargs.items():
            if not key:
                continue
            if key[0] != '_' and key not in ['id','id_', 'd_id','name','sequence_id','table','column']:       
                setattr(row, key, value)
     
        self._seen_tables[name] = row


        if extant:
            self.bundle.database.session.merge(row)
        else:
            self.bundle.database.session.add(row)

        return row

    def add_column(self, table, name,  **kwargs):
        '''Add a column to the schema'''

        # Make sure that the columnumber is monotonically increasing
        # when it is specified, and is one more than the last one if not.

        import pdb;



        if not table.name in self.max_col_id:
            if len(table.columns) == 0:
                self.max_col_id[table.name] = 0
            elif len(table.columns) == 1:
                self.max_col_id[table.name] = table.columns[0].sequence_id
            else:
                self.max_col_id[table.name] = max(*[c.sequence_id for c in table.columns])

        sequence_id = int(kwargs['sequence_id']) if 'sequence_id' in kwargs and kwargs['sequence_id'] is not None else None


        if sequence_id is None:
            sequence_id = self.max_col_id[table.name] + 1

        elif sequence_id <= self.max_col_id[table.name]:
                raise Exception("Column {} specifies column number {}, but last number in table {} is {}"
                            .format(name, sequence_id, table.name, self.max_col_id[table.name]))


        self.max_col_id[table.name] = sequence_id
        kwargs['sequence_id'] = sequence_id

        c =  table.add_column(name, **kwargs)

        return c

    def remove_table(self, table_name):
        from orm import Table, Column
        from sqlalchemy.orm.exc import NoResultFound

        s = self.bundle.database.session

        try:
            table = (s.query(Table).filter(Table.name == table_name)).one()
        except NoResultFound:
            table = None

        if not table:
            return

        s.query(Column).filter(Column.t_vid == table.vid).delete()
        s.query(Table).filter(Table.vid == table.vid).delete()

        s.commit()

        if table_name in self._seen_tables:
            del self._seen_tables[table_name]

    @property
    def columns(self):
        '''Return a list of tables for this bundle'''
        from ambry.orm import Column
        return (self.bundle.database.session.query(Column).all())

    @classmethod        
    def validate_column(cls, table, column, warnings, errors):  
  
        # Postgres doesn't allow size modifiers on Text fields.
        if column.datatype == Column.DATATYPE_TEXT and column.size:
            warnings.append((table.name,column.name,"Postgres doesn't allow a TEXT field to have a size. Use a VARCHAR instead."))
        
        # MySql requires that text columns that have a default also have a size. 
        if column.type_is_text() and  bool(column.default):
            if not column.size and not column.width:
                warnings.append((table.name,column.name, "MySql requires a Text or Varchar field with a default to have a size."))
                
            if isinstance(column.default, basestring) and column.width and len(column.default) > column.width :
                warnings.append((table.name,column.name,"Default value is longer than the width"))
                
            if isinstance(column.default, basestring) and column.size and len(column.default) > column.size:
                warnings.append((table.name,column.name,"Default value is longer than the size"))
        
        if column.default:
            try:
                column.python_cast(column.default)
            except TypeError as e:
                errors.append((table.name,column.name,"Bad default value '{}' for type '{}' (T); {}".format(column.default, column.datatype, e)))
            except ValueError:
                errors.append((table.name,column.name,"Bad default value '{}' for type '{}' (V)".format(column.default, column.datatype)))

    @classmethod        
    def translate_type(cls,driver, table, column):
        '''Translate types for particular driver, and perform some validity checks'''
        # Creates a lot of unnecessary objects, but speed is not important here.  
        
        if driver == 'postgis':
            driver = 'postgres'

        if driver == 'mysql':
            
            if (column.datatype in (Column.DATATYPE_TEXT, column.datatype == Column.DATATYPE_VARCHAR) and
                bool(column.default) and not bool(column.size) and not bool(column.width) ):
                raise ConfigurationError("Bad column {}.{}: For MySql, text columns with default must also have size or width"
                                         .format(table.name, column.name))

            if (column.datatype in (Column.DATATYPE_TEXT, column.datatype == Column.DATATYPE_VARCHAR) and bool(column.default) 
                and not bool(column.size) and bool(column.width)):
                    column.size = column.width
           
                
            # Mysql, when running on Windows, does not allow default
            # values for TEXT columns
            if (column.datatype == Column.DATATYPE_TEXT  and bool(column.default)):
                column.datatype = Column.DATATYPE_VARCHAR
              
            # VARCHAR requires a size
            if (column.datatype == Column.DATATYPE_VARCHAR and not bool(column.size)):
                column.datatype = Column.DATATYPE_TEXT                 
                
        # Postgres doesn't allows size specifiers in TEXT columns. 
        if driver == 'postgres':
            if (column.datatype == Column.DATATYPE_TEXT  and bool(column.size)):
                column.datatype = Column.DATATYPE_VARCHAR            
              
        if driver == 'sqlite' or driver != 'postgres' :
            if column.is_primary_key and column.datatype == Column.DATATYPE_INTEGER64:
                column.datatype = Column.DATATYPE_INTEGER # Required to trigger autoincrement
              
              
        #print driver, column.name, column.size, column.default
                
        type_ =  Column.types[column.datatype][0]


        if column.datatype == Column.DATATYPE_NUMERIC:
            return type_(column.precision, column.scale)
        elif column.size and column.datatype != Column.DATATYPE_INTEGER:
            try:
                return type_(column.size)
            except TypeError: # usually, the type does not take a size
                return type_
        else:
            return type_

    @staticmethod
    def munge_index_name(table, n, alt=None):
        if alt:
            return alt + '_' + n
        else:
            return table.vid_enc + '_' + n


    def get_table_meta(self, name_or_id, use_id=False, driver=None, alt_name=None):
        '''Method version of get_table_meta_from_db'''
        return self.get_table_meta_from_db(self.bundle.database, name_or_id, use_id, driver,
                                           session = self.bundle.database.session,
                                           alt_name = alt_name)

    @classmethod
    def get_table_meta_from_db(self,db,  name_or_id,  use_id=False, 
                               driver=None, d_vid = None, session=None, alt_name=None ):
        '''
            use_id: prepend the id to the class name
        '''

        from sqlalchemy import MetaData, UniqueConstraint, Index, text
        from sqlalchemy import Column as SAColumn
        from sqlalchemy import Table as SATable
        
        metadata = MetaData()
        
        table = self.get_table_from_database(db, name_or_id, d_vid = d_vid, session=session)

        if alt_name and use_id:
            raise ConfigurationError("Can't specify both alt_name and use_id")

        if alt_name:
            table_name = alt_name
        elif use_id:
            table_name = table.vid.replace('/','_')+'_'+table.name
        else:
            table_name = table.name
        
        at = SATable( table_name, metadata)
 
        indexes = {}
        uindexes = {}
        constraints = {}
        foreign_keys = {}
       
        for column in table.columns:
            
            kwargs = {}
        
            width = column.size if column.size else (column.width if column.width else None)
        
            if column.default is not None:
                
                try:
                    int(column.default)
                    kwargs['server_default'] = text(str(column.default))
                except:
                    
                    # Stop-gap for old ambry. This should be  ( and now is ) checkd in the
                    # schema generation
                    if  width and width < len(column.default):
                        raise Exception("Width smaller than size of default for column: {}".format(column.name))
                        
                        
                    kwargs['server_default'] = column.default
          
          
            tt = self.translate_type(driver, table, column)

            ac = SAColumn(column.name, 
                          tt, 
                          primary_key = ( column.is_primary_key == 1),
                          **kwargs
                          )

            at.append_column(ac)

            if column.foreign_key:
                fk = column.foreign_key
                fks = "{}.{}_id".format(fk.capitalize(), fk)
                foreign_keys[column.name] = fks
           
            # assemble non unique indexes
            if column.indexes and column.indexes.strip():
                for cons in column.indexes.strip().split(','):
                    if cons.strip() not in indexes:
                        indexes[cons.strip()] = []
                    indexes[cons.strip()].append(ac)

            # assemble  unique indexes
            if column.uindexes and column.uindexes.strip():
                for cons in column.uindexes.strip().split(','):
                    if cons.strip() not in uindexes:
                        uindexes[cons.strip()] = []
                    uindexes[cons.strip()].append(ac)


            # Assemble constraints
            if column.unique_constraints and column.unique_constraints.strip(): 
                for cons in column.unique_constraints.strip().split(','):
                    
                    if cons.strip() not in constraints:
                        constraints[cons.strip()] = []
                    
                    constraints[cons.strip()].append(ac)


        # Append constraints. 
        for constraint, columns in constraints.items():
            at.append_constraint(UniqueConstraint(name=self.munge_index_name(table, constraint, alt=alt_name),*columns))
             
        # Add indexes   
        for index, columns in indexes.items():
            Index(self.munge_index_name(table, index, alt=alt_name), unique = False ,*columns)
    
        # Add unique indexes   
        for index, columns in uindexes.items():
            Index(self.munge_index_name(table, index, alt=alt_name), unique = True ,*columns)
        
        #for from_col, to_col in foreign_keys.items():
        #    at.append_constraint(ForeignKeyConstraint(from_col, to_col))
        
        return metadata, at
 
    def generate_indexes(self, table):
        """Used for adding indexes to geo partitions. Generates index CREATE commands"""

        indexes = {}
        uindexes = {}
        
        for column in table.columns:
            # assemble non unique indexes
            if column.indexes and column.indexes.strip():
                for cons in column.indexes.strip().split(','):
                    if cons.strip() not in indexes:
                        indexes[cons.strip()] = set()
                    indexes[cons.strip()].add(column)

            # assemble  unique indexes
            if column.uindexes and column.uindexes.strip():
                for cons in column.uindexes.strip().split(','):
                    if cons.strip() not in uindexes:
                        uindexes[cons.strip()] = set()
                    uindexes[cons.strip()].add(column)

        for index_name, cols in indexes.items():
            index_name = self.munge_index_name(table, index_name)
            yield "CREATE INDEX IF NOT EXISTS {} ON {} ({});".format(index_name, table.name,  
                                                                     ','.join([c.name for c in cols]) )
            
        for index_name, cols in uindexes.items():
            index_name = self.munge_index_name(table, index_name)
            yield "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});".format(index_name, table.name, 
                                                                             ','.join([c.name for c in cols]) )

                    
    def create_tables(self):
        '''Create the defined tables as database tables.'''
        with self.bundle.session:
            for t in self.tables:
                if not t.name in self.bundle.database.inspector.get_table_names():
                    t_meta, table = self.bundle.schema.get_table_meta(t.name) #@UnusedVariable
                    table.create(bind=self.bundle.database.engine)


    def caster(self, table_name):
        '''Return a caster for a table. This is like orm.Table.caster, but it will use special caster types
        defined in the schema'''

        from ambry.transform import CasterTransformBuilder

        # The session isn't needed in
        #with self.bundle.session as s:

        table = self.table(table_name)

        bdr = CasterTransformBuilder()

        for c in table.columns:

            # Try to get a caster type object from the bundle
            if 'caster' in c.data and c.data['caster']:
                t = None

                for l in [self.bundle, self.bundle.__module__]:
                    try:
                        t = getattr(self.bundle,c.data['caster'] )
                        bdr.add_type(t)
                        break
                    except AttributeError:
                        continue

                if not t:
                    self.bundle.error("Schema declared undefined caster type {} for {}.{}. Ignoring"
                                      .format(c.data['caster'], table.name, c.name))
                    t = c.python_type

            else:
                t = c.python_type

            bdr.append(c.name, t)

        return bdr


    def schema_from_file(self, file_, progress_cb=None):

        if not progress_cb:
            progress_cb = self.bundle.init_log_rate(N=20)

        return self._schema_from_file(file_, progress_cb)

        
    def _schema_from_file(self, file_, progress_cb=None):
        '''Read a CSV file, in a particular format, to generate the schema'''
        from orm import Column
        import csv, re
        
        # Not using this!! Because it failed to handle commas inside double quotes. 
        try:
            dlct = csv.Sniffer().sniff(file_.read(2024))
        except:
            dlct = None
            
        file_.seek(0)

        if not progress_cb:
            def progress_cb():
                pass

        reader  = csv.DictReader(file_)

        t = None

        new_table = True
        last_table = None
        line_no = 1 # Accounts for file header. Data starts on line 2

        errors = []
        warnings = []
    
        for row in reader:

            line_no += 1

            if not row.get('column', False) and not row.get('table', False):
                continue
            
            row = { k:str(v).decode('utf8', 'ignore').encode('ascii','ignore').strip() for k,v in row.items() }

            if  row['table'] and row['table'] != last_table:
                new_table = True
                last_table = row['table']
            
            if new_table and row['table']:
                
                progress_cb("Add schema table: {}".format(row['table']))
                
                try: table =  self.table(row['table'])
                except: table = None 
                
                if table:
                    errors.append((table.name,None,"Table already exists"))
                    return warnings, errors 
   
                try:
                    t = self.add_table(row['table'], **row)
                except Exception as e:
                    errors.append((None,None," Failed to add table: {}. Row={}. Exceptoin={}".format(row['table'], row, e)))
                    return warnings, errors
                
                new_table = False
              
            # Ensure that the default doesnt get quotes if it is a number. 
            if row.get('default', False):
                try:
                    default = int(row['default'])
                except:
                    default = row['default']
            else:
                default = None
            
            if not row.get('column', False):
                raise ConfigurationError("Row error: no column on line {}".format(line_no))
            if not row.get('table', False):
                raise ConfigurationError("Row error: no table on line {}".format(line_no))
            if not row.get('type', False):
                raise ConfigurationError("Row error: no type on line {}".format(line_no))

            indexes = [ row['table']+'_'+c for c in row.keys() if (re.match('i\d+', c) and _clean_flag(row[c]))]  
            uindexes = [ row['table']+'_'+c for c in row.keys() if (re.match('ui\d+', c) and _clean_flag(row[c]))]  
            uniques = [ row['table']+'_'+c for c in row.keys() if (re.match('u\d+', c) and  _clean_flag(row[c]))]  

            datatype = row['type'].strip().lower()
         
            width = _clean_int(row.get('width', None))
            size = _clean_int(row.get('size',None))
            start = _clean_int(row.get('start', None))
            
            if  width and width > 0:
                illegal_value = '9' * width
            else:
                illegal_value = None
            
            data = { k.replace('d_','',1): v for k,v in row.items() if k.startswith('d_') }
            
            description = row.get('description','').strip().encode('utf-8')

            #progress_cb("Column: {}".format(row['column']))

            col = self.add_column(t,row['column'],
                                   sequence_id = row.get('seq',None),
                                   is_primary_key= True if row.get('is_pk', False) else False,
                                   foreign_key= row['is_fk'] if row.get('is_fk', False) else None,
                                   description=description,
                                   datatype=datatype,
                                   unique_constraints = ','.join(uniques),
                                   indexes = ','.join(indexes),
                                   uindexes = ','.join(uindexes),
                                   default = default,
                                   illegal_value = illegal_value,
                                   size = size,
                                   start = start,
                                   width = width,
                                   data=data,
                                   sql=row.get('sql',None),
                                   precision=int(row['precision']) if row.get('precision',False) else None,
                                   scale=float(row['scale']) if row.get('scale',False) else None,
                                   flags=row.get('flags',None),
                                   keywords=row.get('keywords',None),
                                   measure=row.get('measure',None),
                                   units=row.get('units',None),
                                   universe=row.get('universe',None)
                                   )

            self.bundle.database.session.commit()

            
            self.validate_column(t, col, warnings, errors)

        return warnings, errors

    def extract_schema(self, db):
        '''Extract an Ambry schema from a database and create it in this bundle '''

        for table_name in db.inspector.get_table_names():
            self.bundle.log("Extracting: {}".format(table_name))

            t = self.add_table(table_name)

            for c in db.inspector.get_columns(table_name):

                name = c['name']

                try:
                    size = c['type'].length
                    dt = str(c['type']).replace(str(size),'').replace('()','').lower()
                except AttributeError:
                    size = None
                    dt = str(c['type']).lower()

                self.bundle.log("   {} {} {} ".format(name, dt, size))

                self.add_column(t, name, datatype = dt,
                                size = size,
                                is_primary_key = c['primary_key'] != 0)



    def write_schema(self):
        '''Write the schema back to the schema file'''
        with open(self.bundle.filesystem.path('meta',self.bundle.SCHEMA_FILE), 'w') as f:
            self.as_csv(f)

    def copy_table(self, in_table, out_table_name=None):
        '''Copy a table schema into this schema'''

        if not out_table_name:
            out_table_name = in_table.name

        with self.bundle.session as s:

            cols = []
            for c in in_table.columns:
                d = c.dict

                del d['t_vid']
                del d['vid']
                cols.append(d)

            table = self.add_table(out_table_name, data=in_table.data)
            for c in cols:
                self.add_column(table, **c)

    def _dump_gen(self, table_name=None):
        """Yield schema row for use in exporting the schem to other
        formats

        """

        # Collect indexes
        indexes = {}

        all_opt_col_fields = ["size", "precision","scale", "default","start", "width",
                              "description","sql","flags","keywords",
                              "measure","units","universe"]
        
        opt_col_fields = []
        
        if table_name:
            tables = [ table for table in self.tables if table.name == table_name]
        else:
            tables = self.tables
            
        
        data_fields = set()
        # Need to get all of the indexes figured out first, since there are a variable number of indexes.
        for table in tables:

            for col in table.columns: 
                
                for index_set in [col.indexes, col.uindexes, col.unique_constraints]:
                    if not index_set:
                        continue # HACK. This probably shouldnot happen
                    for idx in index_set.split(','):
                        
                        idx = idx.replace(table.name+'_','')
                        if not idx in indexes:
                            indexes[idx] = set()
                            
                        indexes[idx].add(col)
                    
                for field in all_opt_col_fields:

                    v = getattr(col, field)
                    if v and field not in opt_col_fields:
                        opt_col_fields.append(field)

                for k,v in col.data.items():
                    data_fields.add(k) 

        # also add data columns for the table
        
            for k,v in table.data.items():
                data_fields.add(k)

            
        data_fields = sorted(data_fields)
                     
        # Put back into same order as in all_opt_col_fields
        opt_col_fields = [ field for field in all_opt_col_fields if field in opt_col_fields]

        indexes = OrderedDict(sorted(indexes.items(), key=lambda t: t[0]))

        first = True
        
        for table in tables:
            
            for col in table.columns:
                row = OrderedDict()
                row['table'] = table.name
                row['seq'] = col.sequence_id
                row['column'] = col.name
                row['is_pk'] = 1 if col.is_primary_key else ''
                row['is_fk'] = col.foreign_key if col.foreign_key else None
                row['type'] = col.datatype.upper()   if col.datatype else None

                for idx,s in indexes.items():
                    if idx:
                        row[idx] = 1 if col in s else None

                for field in opt_col_fields:
                    row[field] = getattr(col, field)

                if col.is_primary_key:
                    # For the primary key, the data comes from the table. 
                    for k in data_fields:
                        row['d_'+k]=table.data.get(k,None)
                else:
                    for k in data_fields:
                        row['d_'+k]=col.data.get(k,None)

                row['description'] = col.description

                row['id'] = col.id_

                if first:
                    first = False
                    yield row.keys()
                    
                yield row
  
             
    def as_csv(self, f = None):
        import unicodecsv as csv
        import sys
        
        if f is None:
            f = sys.stdout

        g = self._dump_gen()
        
        try:
            header = g.next()
        except StopIteration:
            # No schema file at all!
            return 
            
        w = csv.DictWriter(f,header, encoding='utf-8')
        w.writeheader()
        last_table = None
        for row in g:
            
            # Blank row to seperate tables. 
            if last_table and row['table'] != last_table:
                w.writerow({})
            
            w.writerow(row)
        
            last_table = row['table']
             
    def as_gs_json(self, table):

        
        g = self._dump_gen()
        
        header = g.next()

        last_table = None
        for row in g:
            
            # Blank row to seperate tables. 
            if last_table and row['table'] != last_table:
                pass
            last_table = row['table']
             
    def as_struct(self):

        
        class GrowingList(list): # http://stackoverflow.com/a/4544699/1144479
            def __setitem__(self, index, value):
                if index >= len(self):
                    self.extend([None]*(index + 1 - len(self)))
                list.__setitem__(self, index, value)
        
        o = defaultdict(GrowingList)
        
        g = self._dump_gen()
        
        header = g.next()
    
        for row in g:
            o[row['table']][row['seq']-1] = row
            
        return o      
        
    def as_text(self, table, pad = '    '):
        import textwrap
        
        g = self._dump_gen(table_name=table)
        
        header = g.next()
    
        rows = []
        rows.append(['#', 'Id', 'Column', 'Type', 'Size','Description'])
    
        def fill(row, sizes):
            return [ str(cell).ljust(size) for cell, size in zip(row, sizes)]
    
        out = "### {} table\n".format(table.title())

        for row in g: 
            
            if  'size' not in row or  row['size'] is None:
                row['size'] = ''

                   
            rows.append([row['seq'], row['id'], row['column'], row['type'].title(), row['size'], row['description'] ])

        desc_wrap = 40
        
        sizes = [0] * len(rows[0])
        for row in rows:
            for i, cell in enumerate(row):
                sizes[i] = max(sizes[i], len(str(cell)))

                
        sizes[-1] = desc_wrap

        out += pad + '  '.join(fill(rows.pop(0), sizes))+'\n'
        
        lines = pad + '-+'.join( [ '-'*size for size in sizes ]) +'\n'
        out += lines

        for row in rows:
            
            # Handling the wrapping of the description is tricky. 
            if row[-1]:
                drows = textwrap.wrap(row[-1], desc_wrap) # Put in the first row of the wrapped desc
                row[-1] = drows.pop(0)
            else:
                drows = []
                
            join_str = '  '
            
            out += pad + join_str.join(fill(row, sizes))+'\n'
            
            # Now add in all of the other rows. 
            if drows:
                row.pop() # Get rid of the desc, so we can get the length of the padding for subsequent rows. 
                dsizes = list(sizes)
                dsizes.pop()
                desc_pad = ' ' * len(pad + join_str.join(fill(row, dsizes)) )
                for desc in drows:
                    out += desc_pad + join_str+ desc + "\n"
            
            #out += lines

        return out
        

                
    def as_orm(self):
        """Return a string that holds the schema represented as Sqlalchemy
        classess"""


        def write_file():
            return """
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, Boolean
from sqlalchemy import Float as Real,  Text, ForeignKey
from sqlalchemy.orm import relationship, backref, deferred
from sqlalchemy.types import TypeDecorator, TEXT, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import Mutable

Base = declarative_base()

"""


        def write_class(table):
            return """
class {name}(Base):
    __tablename__ = '{tablelc}'
""".format(name=table.name.capitalize(), tablelc=table.name.lower())
        
        def write_fields(table):
            import re
            
            o = ""
            for col in table.columns:
                opts = []
                optstr = ''

                if col.is_primary_key: opts.append("primary_key=True") 
                if col.foreign_key: opts.append("ForeignKey('{tablelc}')".format(
                                                    tableuc=col.foreign_key.capitalize(), tablelc=col.foreign_key)) 
                
                if  len(opts):
                    optstr = ',' + ','.join(opts)
                  
                o += "    {column} = SAColumn('{column}',sqlalchemy.types.{type}{options})\n".format(
                                            column=col.name, type=col.sqlalchemy_type.__name__,options=optstr)
            
            for col in table.columns:
                if col.foreign_key:
                    rel_name = re.sub('_id$', '', col.name)
                    
                    t = """
    {rel_name}=relationship(\"{that_table_uc}\",
       foreign_keys=[{column}],
       backref=backref('{this_table}_{rel_name}', 
                       order_by='{that_table_lc}'))
"""
                    #t = "    {rel_name}=relationship(\"{that_table_uc}\")\n"
                    
                    o += t.format(
                           column=col.name, 
                           that_table_uc=col.foreign_key.capitalize(), 
                           that_table_lc=col.foreign_key.lower(), 
                           this_table = table.name,
                           rel_name = rel_name
                     )

            return o
        
        def write_init(table):
            o = "    def __init__(self,**kwargs):\n"
            for col in table.columns:
                o += "        self.{column} = kwargs.get(\"{column}\",None)\n".format(column=col.name)
            
            return o

        out = write_file()
        for table in self.tables:
            out += write_class(table)
            out += "\n"
            out += write_fields(table)
            out += "\n"
            out += write_init(table)
            out += "\n\n"

        return out
    
    def write_orm(self):
        """Writes the ORM file to the lib directory, which is automatically added to the
        import path by the Bundle"""
        import os
        
        lib_dir = self.bundle.filesystem.path('lib')
        if not os.path.exists(lib_dir):
            os.makedirs(lib_dir)
            
        with open(os.path.join(lib_dir,'orm.py'),'w') as f:
            f.write(self.as_orm())

    def _repr_html_(self):

        out = ''

        for t in self.tables:
            out += '\n<h2>{}</h2>\n'.format(t.name)
            out += t._repr_html_()

        return out


    def add_views(self):
        """Add views defined in the configuration"""
        
        for p in self.bundle.partitions:

            if not p.table:
                continue
  
            if not self.bundle.config.group('views'):
                raise ConfigurationError('add_views() requires views to be specified in the configuration file')
                
            views = self.bundle.config.views.get(p.table.name, False)
 
            if not views:
                continue
            
            for name, view in views.items():
                self.bundle.log("Adding view: {} to {}".format(name, p.identity.name))
                sql = "DROP VIEW IF EXISTS {}; ".format(name)
                p.database.connection.execute(sql)
                  
                sql = "CREATE VIEW {} AS {};".format(name, view)
                p.database.connection.execute(sql)  
        
    def update_lengths(self, table_name,  lengths):
        '''Update the sizes of the columns in table with a dict mapping column names to length'''

        with self.bundle.session as s:

            table = self.table(table_name)
            
            for c in table.columns:
                
                size = lengths.get(c.name, False)
                
                if size and size > c.size:
                    self.bundle.log("Updating schema column length {}.{} {} -> {}".format(table.name, c.name, c.size,size))
                    c.size = size
                    
                    # Integers that are too long for 32 bits should be upgraded to 64
                    if c.datatype == c.DATATYPE_INTEGER and c.size >= 10: #  2^32 is 10 gigits
                        self.bundle.log("Updating schema column datatype {}.{} to integer64".format(table.name, c.name, c.size,size))
                        c.datatype = c.DATATYPE_INTEGER64
                    
                    s.merge(c)


        # Need to expire the unmanaged cache, or the regeneration of the schema in _revise_schema will 
        # use the cached schema object rather than the ones we just updated. 
        self.bundle.database.session.expire_all()
    
    
    def extract_columns(self, extract_table, extra_columns=None):
            
        et = self.table(extract_table)

        if not et:
            raise Exception("Didn't find extract table {}".format(extract_table))

        lines = []
        for col in et.columns: 
            if col.sql:
                sql = col.sql
            else:
                sql = col.name

            lines.append("CAST({sql} AS {type}) AS {col}".format(sql=sql, col=col.name,type=col.schema_type))
            
        if extra_columns:
            lines = lines + extra_columns
           
        return ',\n'.join(lines) 
        
    def extract_query(self, source_table, extract_table, extra_columns=None):

        st = self.table(source_table)
            
        return  "SELECT {} FROM {}".format(self.extract_columns(self, extract_table, extra_columns),st.name)
     
    #
    # Updating Schemas
    #
    #
     
    def reset_to_float(self, table_name):
        '''Reset all of the values in the table, except for the primary key
        to floats, in preparation for intuiting the schema ''' 

    @staticmethod
    def _maybe_datetime(v):
        if not isinstance(v, basestring):
            return False
        
        if len(v) > 22:
            # Not exactly correct; ISO8601 allows fractional sections
            # which could result in a longer string. 
            return False
        
        if '-' not in v and ':' not in v:
            return False
        
        for c in set(v): # Set of Unique characters
            if not c.isdigit() and c not in 'T:-Z':
                return False
            
        return True

    @staticmethod
    def _maybe_time(v):
        if not isinstance(v, basestring):
            return False
        
        if len(v) > 15:
            return False
        
        if ':' not in v:
            return False
        
        for c in set(v): # Set of Unique characters
            if not c.isdigit() and c not in 'T:Z.':
                return False
            
        return True
            
    @staticmethod
    def _maybe_date(v):
        if not isinstance(v, basestring):
            return False
        
        if len(v) > 10:
            # Not exactly correct; ISO8601 allows fractional sections
            # which could result in a longer string. 
            return False
        
        if '-' not in v:
            return False
        
        for c in set(v): # Set of Unique characters
            if not c.isdigit() and c not in '-':
                return False
            
        return True
    
    def intuit(self, row, memo):
        '''Accumulate information about a database row to determine the most likely datatype and length for each
        field. 
        
        To use, run in the row loop:
        
            memo = bundle.schema.intuit_schema(row,memo)
            
        For row being either a dict or list of row values. 
        
        To finalize, call with row == None
        
        '''
        from collections import defaultdict, OrderedDict
        from sqlalchemy.engine import RowProxy
        from datetime import datetime, date, time

        if memo is None :
            memo = {'fields' : None, 'name_index': {}}
            
            memo['fields'] = [{'name': None, 
                               'min-type': int, 
                               'prob-type': None, 
                               'major-type': None, 
                               'minor-type': None,
                               'length': 0, 
                               'maybe': {'date':0, 'datetime':0, 'time':0},
                               'n' : 0,
                               'counts': defaultdict(int)} for i in range(len(row))]
            
            if isinstance(row, dict) or isinstance(row, RowProxy):
                for i,name in enumerate(row.keys()):
                    memo['fields'][i]['name'] = name
                    memo['name_index'][name] = i

        if row is None:
            
            # Finalize the run by computing the major/minor ratio of types for each column 
            for i in range(len(memo['fields'])):

                col = memo['fields'][i]

                if len(col['counts']):
                    scounts = sorted(col['counts'].items(), key = lambda x: x[1], reverse = True)
                    col['major-type'] = scounts.pop(0) if len(scounts) else None
                    col['minor-type'] = scounts.pop(0) if len(scounts) else None
                    
                    if (bool(col['major-type']) and bool(col['major-type'][1]) and 
                        bool(col['minor-type']) and bool(col['minor-type'][1])):
                        col['mmr'] = float(col['minor-type'][1]) / float(col['major-type'][1])
                    else:
                        col['mmr'] = None


                if float(col['maybe']['date'])/float(col['n']) > .90:
                    import datetime
                    col['prob-type'] = datetime.date

                elif float(col['maybe']['time'])/float(col['n']) > .90:
                    import datetime
                    col['prob-type'] = datetime.time

                elif float(col['maybe']['datetime'])/float(col['n']) > .90:
                    import datetime
                    col['prob-type'] = datetime.datetime

                elif col['mmr'] is not None and col['mmr'] < .05:
                    col['prob-type'] = col['major-type'][0]

                else:
                    col['prob-type'] = col['min-type']

            return memo

        for i, v in enumerate(row):
            
            # Convert lists to dicts
            if isinstance(row, dict):
                key = v
                i =  memo['name_index'][key]
                v = row[key]

            if isinstance(v, basestring):
                v = v.strip()
           
           
            mfi = memo['fields'][i]
           
            mfi['n'] += 1
           
            # Keep track of the number of possibilities for each
            # field. This is needed to identify fields that are mostly
            # one type ( ie, Integer ) but which have occasional text codes.

            if v is None:
                mfi['counts'][None] += 1

                continue

            try:
                if isinstance(v, (datetime)):
                    mfi['counts'][datetime] += 1
                    mfi['maybe']['datetime'] += 1
                    continue

                if isinstance(v, (date)):
                    mfi['counts'][date] += 1
                    mfi['maybe']['date'] += 1
                    continue

                if isinstance(v, (time)):
                    mfi['counts'][time] += 1
                    mfi['maybe']['time'] += 1
                    continue

                mfi['counts'][str] += 1
                float(v)
                mfi['counts'][float] += 1
                mfi['counts'][str] -= 1
                int(v)
                mfi['counts'][float] -= 1
                mfi['counts'][int] += 1            
            except TypeError:
                pass
            except ValueError:
                pass
            
            # What follows is the normal intuiting process.

            if v is None or v == '-' or v == '':
                continue
            
            elif mfi['min-type'] == str:
                mfi['length'] = max(mfi['length'], len(unicode(v)))
                mfi['maybe']['date'] += 1 if self._maybe_date(v) else 0
                mfi['maybe']['time'] += 1 if self._maybe_time(v) else 0
                mfi['maybe']['datetime'] += 1 if self._maybe_datetime(v) else 0
                continue # No more conversions are possible.

            mfi['length'] = max(mfi['length'], len(str(v))) 
            
            # Find the base type, the most specific type that will hold
            # this data


            if mfi['min-type'] is int:
                try:
                    int(v)      
                    mfi['min-type'] = int

                except (TypeError, ValueError): # Type error for when v is a datetime
                    mfi['min-type'] = float

                    
            if mfi['min-type'] is float or isinstance(v, float):
                try:
                    float(v)
                    mfi['min-type'] = float

                except (TypeError, ValueError): # Type error for when v is a datetime
                    mfi['min-type'] = str



        return memo

    def _update_from_memo(self, table_name,  memo, logger=None):
        '''Update a table schema using a memo from intuit()'''
        from datetime import datetime, time, date
        from sqlalchemy.orm.exc import NoResultFound

        type_map = {int: 'integer', str: 'varchar', float: 'real',
                    datetime: 'datetime', date: 'date', time: 'time'}

        index = memo['name_index'] if len(memo['name_index']) > 0 else None
        fields = memo['fields']

        with self.bundle.session as s:

            try:
                table = self.table(table_name)
            except NoResultFound:
                table = self.add_table(table_name)

            for d in memo['fields']:
                name = d['name']

                if name == 'id':
                    self.add_column(table, 'id', datatype='integer', is_primary_key=True)

                else:
                    i = index[d['name']]
                    # add_column will update existing columns
                    self.add_column(table, d['name'],
                                    datatype = type_map[fields[i]['prob-type']],
                                    size = fields[i]['length'] if fields[i]['prob-type'] == str else None,
                                    data=dict(header=name))

 
    def update(self, table_name, itr, n=None, header=None, logger=None):
        '''Update the schema from an iterator that returns rows. This
        will create a new table with rows that have datatype intuited from the values. '''

        memo = None

        i = 0
        for row in itr:
            i+= 1
            try:
                memo = self.intuit(row, memo)
            except Exception as e:
                self.bundle.error("Error updating row {} of {}: {}\n{}".format(i, table_name, e, row))
                continue

            if logger:
                logger("Schema update, row  {}".format(i))

            if n and i > n:
                break

        memo = self.intuit(None, memo)

        if header:
            for i, name in enumerate(header):

                memo['name_index'][name] = i
                memo['fields'][i]['name'] = name

        self._update_from_memo(table_name, memo)

        with open(self.bundle.filesystem.path('meta',self.bundle.SCHEMA_FILE), 'w') as f:
            self.as_csv(f)        

        return memo

    def update_csv(self, table_name, file_name, n=500, logger=None):
        """Create a new table, or update an old one, from a CSV file. The CSV file must have a header. """
        from collections import OrderedDict

        import csv

        # Get just the header row, so we can se the correct order of columns
        with open(file_name) as f:
            reader = csv.reader(f)
            header = reader.next()

            def itr():
                for i, row in enumerate(reader):
                    if i > n:
                        break

                    yield row

            return self.update(table_name, itr(), header=header)



    def add_code_table(self, source_table_name, source_column_name):
        '''Add a table to the schema for codes. The codes are integers that are associated 
        with strings, usually to indicate exceptional conditions in an integer field. '''
        import json
    
        if self._code_table_cache == None:
            self._code_table_cache = set()
            
            # Load the tables cache from data attached to 
            # the tables in the schema. 
            
            for table in self.tables:
                
                try:
                    self._code_table_cache.add(tuple(json.loads(table.data['code_key'])))
                except KeyError:
                    continue
                except ValueError: # Json decode error
                    continue
            
        
        key = (source_table_name, source_column_name)
        
        if key in self._code_table_cache:
            return
        
        self._code_table_cache.add(key)

        
        name = "{}_{}_codes".format(source_table_name, source_column_name)
        
        self.bundle.log("Adding code table: {}".format(name))
        
        with self.bundle.session as s:

            # This has to come before the add_table, or the self.col_sequence values get screwed up
            table = self.table(source_table_name)
            self.add_column(table, source_column_name+'_code',
                             datatype = 'varchar',
                             description='Non-integer code value for column {}'.format(source_column_name))


            t = self.add_table(name, data={'code_key':json.dumps([source_table_name, source_column_name])})
            self.add_column(t, 'id', datatype='integer', is_primary_key = True)
            self.add_column(t, 'code', datatype='varchar', description='Value of code')
            self.add_column(t, 'description', datatype='varchar', description='Code description')
