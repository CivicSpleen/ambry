"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from collections import MutableMapping

from sqlalchemy import Column as SAColumn
from sqlalchemy import  Text, String, ForeignKey

from . import Base, JSONAlchemy

class Config(Base):

    __tablename__ = 'config'

    d_vid = SAColumn('co_d_vid', String(16),ForeignKey('datasets.d_vid'), primary_key=True,index=True)
    type = SAColumn('co_type', String(200), primary_key=True)
    group = SAColumn('co_group', String(200), primary_key=True)
    key = SAColumn('co_key', String(200), primary_key=True)

    value = SAColumn('co_value', JSONAlchemy(Text()))

    @property
    def dict(self):
        return {p.key: getattr(self, p.key) for p in self.__mapper__.attrs}

    def __repr__(self):
        return "<config: {},{},{} = {}>".format(self.d_vid,self.group,self.key,self.value)

class ConfigTypeGroupAccessor(object):

    def __init__(self, dataset, type_name, group_name,  *args, **kwargs):
        from sqlalchemy.orm import object_session

        self._dataset = dataset
        self._type_name = type_name
        self._group_name = group_name

        self._session = object_session(dataset)

        self._configs = {}

        for config in (self._session.query(Config)
                               .filter(Config.d_vid == self._dataset.vid)
                               .filter(Config.type == self._type_name)
                               .filter(Config.group == self._group_name)).all():

            self._configs[config.key] = config

        self.__initialized = True

    def __getattr__(self, k):

        return self._configs[k].value

    def __setattr__(self, k, v):
        """Maps attributes to values.
        Only if we are initialised
        """
        if not self.__dict__.has_key('_ConfigTypeGroupAccessor__initialized'):
            return object.__setattr__(self, k, v)
        elif self.__dict__.has_key(k):       # any normal attributes are handled normally
            raise AttributeError("Can't set attributed after initialization")
        else:
            if k in self._configs:
                self._configs[k].value = v
                self._session.merge(self._configs[k])
            else:
                config = Config(d_vid = self._dataset.vid, type = self._type_name, group = self._group_name,
                                key = k, value = v)
                self._configs[k] = config
                self._session.add(config)


    def __iter__(self):
        return iter(self._configs.keys())

    def items(self):
        for k,v in self._configs.items():
            yield (k,v.value)

    def records(self):
        for k,v in self._configs.items():
            yield (k,v)

    def __len__(self):
        return len(self._configs)

class ConfigGroupAccessor(object):

    def __init__(self, dataset, type_name):

        self._dataset = dataset
        self._type_name = type_name

    def __getattr__(self, k):

        return ConfigTypeGroupAccessor(self._dataset, self._type_name, k)

    def __setattr__(self, k, v):
        if k.startswith('_'):
            return super(ConfigGroupAccessor, self).__setattr__(k, v)
        else:
            raise AttributeError("Can't set groups in ConfigGroupAccessor")


