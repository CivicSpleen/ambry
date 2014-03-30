

import os
from ..util import lru_cache


@lru_cache()
def partition_classes():
    """Return a holder object that has lists of the known partition types mapped to other keys

    Used for getting a partition class based on simple name, format, extension, etc. """

    from geo import GeoPartitionName,GeoPartitionName,GeoPartition,GeoPartitionIdentity
    from hdf import HdfPartitionName,HdfPartitionName,HdfPartition,HdfPartitionIdentity
    from csv import CsvPartitionName,CsvPartitionName,CsvPartition,CsvPartitionIdentity
    from sqlite import SqlitePartitionName, SqlitePartitionName, SqlitePartition, SqlitePartitionIdentity

    class PartitionClasses(object):
        name_by_format = { pnc.format_name(): pnc for pnc in (GeoPartitionName, HdfPartitionName,
                                                            CsvPartitionName, SqlitePartitionName )}

        extension_by_format = {pc.format_name(): pc.extension() for pc in (GeoPartitionName, HdfPartitionName,
                                                            CsvPartitionName, SqlitePartitionName )}

        partition_by_format = {pc.format_name(): pc for pc in (GeoPartition, HdfPartition,
                                                            CsvPartition, SqlitePartition )}

        identity_by_format = {ic.format_name(): ic for ic in (GeoPartitionIdentity, HdfPartitionIdentity,
                                                            CsvPartitionIdentity, SqlitePartitionIdentity )}


    return PartitionClasses()


def name_class_from_format_name(name):


    if not name:
        name = 'db'

    try:
        return partition_classes().name_by_format[name]
    except KeyError:
        raise KeyError("Unknown format name: {}".format(name))


def partition_class_from_format_name(name):


    if not name:
        name = 'db'

    try:
        return partition_classes().partition_by_format[name]
    except KeyError:
        raise KeyError("Unknown format name: {}".format(name))


def identity_class_from_format_name(name):

    if not name:
        name = 'db'

    try:
        return partition_classes().identity_by_format[name]
    except KeyError:
        raise KeyError("Unknown format name: {}".format(name))


def extension_for_format_name(name):

    if not name:
        name = 'db'

    try:
        return partition_classes().extension_by_format[name]
    except KeyError:
        raise KeyError("Unknown format name: {}".format(name))


def new_partition(bundle, orm_partition, **kwargs):

    cls = partition_class_from_format_name(orm_partition.format)

    return cls(bundle, orm_partition, **kwargs)


def new_identity(d, bundle=None):

    if bundle:
        d = dict(d.items() + bundle.identity.dict.items())

    if not 'format' in d:
        d['format'] = 'db'

    format_name = d['format']

    ic = partition_class_from_format_name(format_name)

    return ic.from_dict(d)


class PartitionInterface(object):

    @property
    def name(self): raise NotImplementedError()
    
    def _path_parts(self): 
        raise NotImplementedError()
    
    @property
    def path(self):
        '''Return a pathname for the partition, relative to the containing 
        directory of the bundle. '''
        raise NotImplementedError()

    def sub_dir(self, *args):
        """Return a subdirectory relative to the partition path"""
        raise NotImplementedError()
    
    @property
    def database(self):  
        raise NotImplementedError()


    def unset_database(self):
        '''Removes the database record from the object'''
        raise NotImplementedError()
    

       
    @property
    def tables(self):  raise NotImplementedError() 

    def create(self):  raise NotImplementedError()
        
    def delete(self):  raise NotImplementedError()
        
    def inserter(self, table_or_name=None,**kwargs):  raise NotImplementedError()

    def updater(self, table_or_name=None,**kwargs):  raise NotImplementedError()

    def write_stats(self, min_key, max_key, count):  raise NotImplementedError()


class PartitionBase(PartitionInterface):

    _db_class = None

    def __init__(self, db, record, **kwargs):
        
        self.bundle = db
        self.record = record

        self.dataset = self.record.dataset
        self.identity = self.record.identity
        self.data = self.record.data

        # These two values take refreshable fields out of the partition ORM record.
        # Use these if you are getting DetatchedInstance errors like:
        #    sqlalchemy.orm.exc.DetachedInstanceError: Instance <Table at 0x1077d5450>
        #    is not bound to a Session; attribute refresh operation cannot proceed
        self.record_count = self.record.count

        #self.table = self.get_table()

        self._database =  None

    @classmethod
    def init(cls, record): 
        record.format = cls.FORMAT

    @property
    def name(self):
        return self.identity.name

    def get(self):
        '''Fetch this partition from the library or remote if it does not exist'''
        import os
        return self.bundle.library.get(self.identity.vid).partition

    @property
    def path(self):
        '''Return a pathname for the partition, relative to the containing 
        directory of the bundle. '''

        return self.bundle.sub_dir(self.identity.sub_path)  #+self._db_class.EXTENSION

    def sub_dir(self, *args):
        """Return a subdirectory relative to the partition path"""
        return  os.path.join(self.path,*args)

    @property
    def table(self):
        return self.get_table()

    @property
    def tables(self):
        return self.data.get('tables',[])


    # Call other values on the record
    def __getattr__(self, name):
        if hasattr(self.record, name):
            return getattr(self.record, name)
        else:
            raise AttributeError('Partition does not have attribute {} '.format(name))


    def get_table(self, table_spec=None):
        '''Return the orm table for this partition, or None if
        no table is specified. 
        '''
        
        if not table_spec:
            table_spec = self.identity.table
            
            if table_spec is None:
                return None
            
        return self.bundle.schema.table(table_spec)


    def unset_database(self):
        '''Removes the database record from the object'''
        self._database = None
       

    
    def inserter(self, table_or_name=None,**kwargs):

        if not self.database.exists():
           self.create()

        return self.database.inserter(table_or_name,**kwargs)


    def updater(self, table_or_name=None, **kwargs):

        if not self.database.exists():
            self.create()

        return self.database.updater(table_or_name, **kwargs)


    def delete(self):
        
        try:
  
            self.database.delete()
            self._database = None
            
            with self.bundle.session as s:
                # Reload the record into this session so we can delete it. 
                from ..orm import Partition
                r = s.query(Partition).get(self.record.vid)
                s.delete(r)

            self.record = None
            
        except:
            raise

    def finalize(self):
        '''Wrap up the creation of this partition'''


    def set_state(self, state):
        from ..orm import Partition

        with self.bundle.session as s:
            r = s.query(Partition).get(self.record.vid)

            if r: # No record for memory partitions
                r.state = state

                s.merge(r)

                s.commit()


    def dbm(self, suffix = None):
        '''Return a DBMDatabase related to this partition'''
        
        from ..database.dbm import Dbm
        
        return Dbm(self.bundle, base_path=self.path, suffix=suffix)
        

    @classmethod
    def format_name(self):
        return self._id_class._name_class.FORMAT

    @classmethod
    def extension(self):
        return self._id_class._name_class.PATH_EXTENSION

    @property
    def info(self):
        """Returns a human readable string of useful information"""

        return ("------ Partition: {name} ------\n".format(name=self.identity.sname)+
        "\n".join(['{:10s}: {}'.format(k,v) for k,v in self.identity.dict.items()])+"\n"
        '{:10s}: {}\n'.format('path',self.database.path)+
        '{:10s}: {}\n'.format('tables', ','.join(self.tables))
        )

    def _repr_html_(self):
        '''IPython display'''
        return "<p>"+self.info.replace("\n","<br/>\n")+"</p>"
