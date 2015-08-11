"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from time import time

from sqlalchemy import Column as SAColumn, Text, String, ForeignKey, Integer, Boolean,\
    event, UniqueConstraint, text
from sqlalchemy.orm import object_session, relationship

from six import iterkeys

from . import Base, JSONAlchemy
from ..identity import ObjectNumber, DatasetNumber



class Config(Base):

    __tablename__ = 'config'
    __table_args__ = (
        UniqueConstraint('co_d_vid', 'co_type', 'co_group', 'co_key', name='_type_group_key_uc'),)

    id = SAColumn('co_id', String(32), primary_key=True)

    d_vid = SAColumn('co_d_vid', String(16), ForeignKey('datasets.d_vid'), index=True, doc='Dataset vid')
    type = SAColumn('co_type', String(200), doc='Type of the config: metadata, process, sync, etc...')
    group = SAColumn('co_group', String(200), doc='Group of the config: identity, about, etc...')
    key = SAColumn('co_key', String(200),  doc='Key of the config')
    value = SAColumn('co_value', JSONAlchemy(Text()),  doc='Value of the config key.')
    modified = SAColumn('co_modified', Integer(),
        doc='Modification date: time in seconds since the epoch as a integer.')

    # FIXME. Foreign key constraints may it hard to dump all of the configs to a new bundle database in
    # ambry.orm.database.Database#copy_dataset, so I've removed the foreign key constraint.

    parent_id = SAColumn(String(32), ForeignKey('config.co_id'), nullable=True, doc='Id of the parent config.')

    parent = relationship('Config',  remote_side=[id])
    children = relationship('Config')

    @property
    def dict(self):
        return {p.key: getattr(self, p.key) for p in self.__mapper__.attrs}

    def __repr__(self):
        return '<config: {},{},{} = {}>'.format(self.d_vid, self.group, self.key, self.value)

    @staticmethod
    def before_insert(mapper, conn, target):

        if not target.id:
            import hashlib
            import time

            assert bool(target.d_vid)

            # This is a bit of a mess, much longer than it should be. Unfortunately, there seem to be few other options
            # since the trick used in partitions doesn't work for mass updates to many configs.
            target.id = hashlib.md5(
                "{}{}{}{}{}".format(target.d_vid, target.type, target.group, target.key, time.time())
            ).hexdigest()

        Config.before_update(mapper, conn, target)

    @staticmethod
    def before_update(mapper, conn, target):

        if object_session(target).is_modified(target, include_collections=False):
            target.modified = time()


event.listen(Config, 'before_insert', Config.before_insert)
event.listen(Config, 'before_update', Config.before_update)


class ConfigTypeGroupAccessor(object):

    def __init__(self, dataset, type_name, group_name,  *args, **kwargs):

        self._dataset = dataset
        self._type_name = type_name
        self._group_name = group_name

        self._session = object_session(dataset)

        assert self._session, 'Dataset has no session'

        # find all matched configs and populate configs cache.


        configs =  (self._session.query(Config)
            .filter_by(d_vid=self._dataset.vid,
                       type=self._type_name,
                       group=self._group_name)).all()

        self._configs = {}

        for config in configs:
            self._configs[config.key] = config

        self.__initialized = True

    def __getattr__(self, k):

        try:
            return self._configs[k].value
        except KeyError:
            return None

    def __setattr__(self, k, v):
        """ Maps attributes to values.
        Only if we are initialised
        """
        if '_ConfigTypeGroupAccessor__initialized' not in self.__dict__:
            return object.__setattr__(self, k, v)
        elif k in self.__dict__:       # any normal attributes are handled normally
            raise AttributeError("Can't set attributed after initialization")
        else:
            if k in self._configs:
                # key exists in the cache, update
                self._configs[k].value = v
                self._session.merge(self._configs[k])
            else:
                # key does not exist in the cache, create new.
                config = Config(
                    d_vid=self._dataset.vid, type=self._type_name,
                    group=self._group_name, key=k, value=v)
                self._configs[k] = config
                self._session.add(config)

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    def __iter__(self):
        return iterkeys(self._configs)

    def items(self):
        for k, v in list(self._configs.items()):
            yield (k, v.value)

    def records(self):
        for k, v in list(self._configs.items()):
            yield (k, v)

    def __len__(self):
        return len(self._configs)


class ConfigGroupAccessor(object):

    def __init__(self, dataset, type_name):

        self._dataset = dataset
        self._type_name = type_name

    def clean(self):
        for config in [config for config in self._dataset.configs if config.type == self._type_name]:
            self._dataset.configs.remove(config)

    def __getattr__(self, k):
        return ConfigTypeGroupAccessor(self._dataset, self._type_name, k)

    def __setattr__(self, k, v):
        if k.startswith('_'):
            return super(ConfigGroupAccessor, self).__setattr__(k, v)
        else:
            raise AttributeError("Can't set groups in ConfigGroupAccessor")

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)
