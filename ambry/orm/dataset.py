"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'



from sqlalchemy import Column as SAColumn, Integer
from sqlalchemy import  String
from sqlalchemy.orm import relationship
from ..util import Constant
from ..identity import LocationRef

from . import Base, MutationDict, JSONEncodedObj

from ambry.identity import DatasetNumber
from ambry.identity import  ObjectNumber
from ambry.orm.file import File
from sqlalchemy.orm import object_session

class Dataset(Base):
    __tablename__ = 'datasets'

    LOCATION = Constant()
    LOCATION.LIBRARY = LocationRef.LOCATION.LIBRARY
    LOCATION.PARTITION = LocationRef.LOCATION.PARTITION
    LOCATION.SREPO = LocationRef.LOCATION.SREPO
    LOCATION.SOURCE = LocationRef.LOCATION.SOURCE
    LOCATION.REMOTE = LocationRef.LOCATION.REMOTE
    LOCATION.UPSTREAM = LocationRef.LOCATION.UPSTREAM

    vid = SAColumn('d_vid', String(20), primary_key=True)
    id = SAColumn('d_id', String(20), )
    name = SAColumn('d_name', String(200), nullable=False, index=True)
    vname = SAColumn('d_vname',String(200),unique=True,nullable=False,index=True)
    fqname = SAColumn('d_fqname', String(200), unique=True, nullable=False)
    cache_key = SAColumn('d_cache_key',String(200),unique=True,nullable=False,index=True)
    source = SAColumn('d_source', String(200), nullable=False)
    dataset = SAColumn('d_dataset', String(200), nullable=False)
    subset = SAColumn('d_subset', String(200))
    variation = SAColumn('d_variation', String(200))
    btime = SAColumn('d_btime', String(200))
    bspace = SAColumn('d_bspace', String(200))
    revision = SAColumn('d_revision', Integer, nullable=False)
    version = SAColumn('d_version', String(20), nullable=False)

    data = SAColumn('d_data', MutationDict.as_mutable(JSONEncodedObj))

    path = None  # Set by the Library and other queries.

    tables = relationship("Table",backref='dataset',cascade="save-update, delete, delete-orphan")

    partitions = relationship("Partition",backref='dataset',cascade="delete, delete-orphan")

    configs = relationship('Config', backref='dataset', cascade="all, delete-orphan")

    files = relationship('File', backref='dataset', cascade="all, delete-orphan")

    colmaps = relationship('ColumnMap', backref='dataset', cascade="all, delete-orphan")

    sources = relationship('DataSource', backref='dataset', cascade="all, delete-orphan")


    _database = None # Reference to the database, when dataset is retrieved from

    def __init__(self, *args, **kwargs):

        super(Dataset, self).__init__(*args, **kwargs)

        if self.vid and not self.id:
            self.revision = ObjectNumber.parse(self.vid).revision
            self.id = str(ObjectNumber.parse(self.vid).rev(None))

        if not self.id:
            dn = DatasetNumber(None, self.revision)
            self.vid = str(dn)
            self.id = str(dn.rev(None))
        elif not self.vid:
            try:
                self.vid = str(ObjectNumber.parse(self.id).rev(self.revision))
            except ValueError as e:
                raise ValueError('Could not parse id value; ' + e.message)

        if self.cache_key is None:
            self.cache_key = self.identity.cache_key

        if not self.name:
            self.name = str(self.identity.name)

        if not self.vname:
            self.vname = str(self.identity.vname)

        if not self.fqname:
            self.fqname = str(self.identity.fqname)

        if not self.version:
            self.version = str(self.identity.version)

        assert self.vid[0] == 'd'

    def commit(self):
        self._database.commit()

    @property
    def identity(self):
        from ..identity import Identity
        return Identity.from_dict(self.dict)

    @property
    def config(self ):
        return ConfigAccessor(self)

    def table(self, ref):
        # AFAIK, all of the columns in the relationship will get loaded if any one is accessed,
        # so iterating over the collection only involves one SELECT.

        from exc import NotFoundError

        for t in self.tables:
            if str(ref) == t.name or str(ref) == t.id or str(ref) == t.vid:
                return t

        raise NotFoundError("Failed to find table for ref '{}' in dataset '{}'".format(ref, self.name))

    def add_table(self, name, add_id=False, **kwargs):
        '''Add a table to the schema, or update it it already exists.

        If updating, will only update data.
        '''
        from . import Table
        from exc import NotFoundError

        try:
            table = self.table(name)
            extant = True
        except NotFoundError:
            extant = False
            table = Table(d_id = self.id, d_vid = self.vid, name = name,
                          sequence_id = len(self.tables)+1)

        # Update possibly extant data
        table.data = dict( (table.data.items() if table.data else []) + kwargs.get('data', {}).items() )

        for key, value in kwargs.items():

            if not key:
                continue
            if key[0] != '_' and key not in ['vid', 'id', 'id_', 'd_id', 'name', 'sequence_id', 'table', 'column', 'data']:
                setattr(table, key, value)

        if add_id:
            table.add_id_column()

        if not extant:
            self.tables.append(table)
        else:

            object_session(self).merge(table)

        return table

    def bsfile(self, name):
        """Return a Build Source file ref, creating a new one if the one requested does not exist"""
        from sqlalchemy.orm.exc import NoResultFound

        try:
            fr = (object_session(self).query(File)
                .filter(File.d_vid == self.vid)
                .filter(File.major_type == File.MAJOR_TYPE.BUILDSOURCE)
                .filter(File.minor_type == name).one())
        except NoResultFound:
            fr = File(d_vid = self.vid,
                      major_type = File.MAJOR_TYPE.BUILDSOURCE,
                      minor_type = name,
                      path = name,
                      source = 'fs')
            object_session(self).add(fr)

        return fr

    @property
    def dict(self):
        d = {
            'id': self.id,
            'vid': self.vid,
            'name': self.name,
            'vname': self.vname,
            'fqname': self.fqname,
            'cache_key': self.cache_key,
            'source': self.source,
            'dataset': self.dataset,
            'subset': self.subset,
            'variation': self.variation,
            'btime': self.btime,
            'bspace': self.bspace,
            'revision': self.revision,
            'version': self.version,
        }

        if self.data:
            for k in self.data:
                assert k not in d
                d[k] = self.data[k]

        return d

    def __repr__(self):
        return """<datasets: id={} vid={} name={} source={} ds={} ss={} var={} rev={}>""".format(
            self.id,
            self.vid,
            self.name,
            self.source,
            self.dataset,
            self.subset,
            self.variation,
            self.revision)


class ConfigAccessor(object):

    def __init__(self, dataset):

        self.dataset = dataset

    @property
    def process(self):
        """Access process configuarion values as attributes

        >>> db = Database(self.dsn)
        >>> db.open()
        >>> ds = db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')
        >>> ds.process.build.foo = [1,2,3,4]
        >>> ds.process.build.bar = [5,6,7,8]
        """

        from config import ConfigGroupAccessor

        return ConfigGroupAccessor(self.dataset, 'process')

    @property
    def meta(self):
        """Access process configuarion values as attributes. See self.process
        for a usage example"""

        from config import ConfigGroupAccessor

        return ConfigGroupAccessor(self.dataset, 'metadata')

    @property
    def build(self):
        """Access build configuration values as attributes. See self.process
            for a usage example"""
        from config import ConfigGroupAccessor

        return ConfigGroupAccessor(self.dataset, 'buildstate')

    @property
    def library(self):
        """Access library configuration values as attributes. The library config
         is really only relevant to the root dataset. See self.process for a usage example"""
        from config import ConfigGroupAccessor

        return ConfigGroupAccessor(self.dataset, 'library')

    def rows(self):
        """Return configuration in a form that can be used to reconstitute a
        Metadata object. Returns all of the rows for a dataset.

        This is distinct from get_config_value, which returns the value
        for the library.

        """
        from ambry.orm import Config as SAConfig
        from sqlalchemy import or_

        rows = []

        for r in self.session.query(SAConfig).filter(or_(SAConfig.group == 'config', SAConfig.group == 'process'),
                                                     SAConfig.d_vid == self.dataset.vid).all():

            parts = r.key.split('.', 3)

            if r.group == 'process':
                parts = ['process'] + parts

            cr = ((parts[0] if len(parts) > 0 else None,
                   parts[1] if len(parts) > 1 else None,
                   parts[2] if len(parts) > 2 else None
                   ), r.value)

            rows.append(cr)

        return rows

