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
from ambry.orm.config import Config
from ambry.orm.file import File


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

    path = None  # Set by the LIbrary and other queries.

    tables = relationship("Table",backref='dataset',cascade="delete, delete-orphan")

    partitions = relationship("Partition",backref='dataset',cascade="delete, delete-orphan")

    configs = relationship('Config', backref='dataset', cascade="delete, delete-orphan",
                           primaryjoin="Config.d_vid == Dataset.vid ", foreign_keys="Config.d_vid")

    files = relationship('File', backref='dataset', cascade="delete, delete-orphan",
                         primaryjoin="File.ref == Dataset.vid ", foreign_keys="File.ref")

    #__table_args__ = (
    #    UniqueConstraint('d_vid', 'd_location', name='u_vid_location'),
    #    UniqueConstraint('d_fqname', 'd_location', name='u_fqname_location'),
    #    UniqueConstraint('d_cache_key', 'd_location', name='u_cache_location'),
    #)

    def __init__(self, **kwargs):
        self.id = kwargs.get("oid", kwargs.get("id", None))
        self.vid = str(kwargs.get("vid", None))
        # Deprecated?
        self.location = kwargs.get("location", self.LOCATION.LIBRARY)
        self.name = kwargs.get("name", None)
        self.vname = kwargs.get("vname", None)
        self.fqname = kwargs.get("fqname", None)
        self.cache_key = kwargs.get("cache_key", None)
        self.source = kwargs.get("source", None)
        self.dataset = kwargs.get("dataset", None)
        self.subset = kwargs.get("subset", None)
        self.variation = kwargs.get("variation", None)
        self.btime = kwargs.get("btime", None)
        self.bspace = kwargs.get("bspace", None)
        self.revision = kwargs.get("revision", None)
        self.version = kwargs.get("version", None)


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

    @property
    def identity(self):
        from ..identity import Identity
        return Identity.from_dict(self.dict)

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

    def config(self,  key, group = 'config'):
        from sqlalchemy.orm import object_session
        s = object_session(self)
        return (s.query(Config)
              .filter(Config.d_vid == self.vid)
              .filter(Config.group == group )
              .filter(Config.key == key )
              .one())

    def get_config_rows(self, d_vid):
        """Return configuration in a form that can be used to reconstitute a
        Metadata object. Returns all of the rows for a dataset.

        This is distinct from get_config_value, which returns the value
        for the library.

        """
        from ambry.orm import Config as SAConfig
        from sqlalchemy import or_

        rows = []

        for r in self.session.query(SAConfig).filter(or_(SAConfig.group == 'config', SAConfig.group == 'process'),
                                                     SAConfig.d_vid == d_vid).all():

            parts = r.key.split('.', 3)

            if r.group == 'process':
                parts = ['process'] + parts

            cr = ((parts[0] if len(parts) > 0 else None,
                   parts[1] if len(parts) > 1 else None,
                   parts[2] if len(parts) > 2 else None
                   ), r.value)

            rows.append(cr)

        return rows