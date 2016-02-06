"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from time import time

from sqlalchemy import Column as SAColumn, Text, String, ForeignKey, Integer,\
    event, UniqueConstraint
from sqlalchemy.orm import object_session, relationship

from six import iterkeys, u

from ambry.orm import next_sequence_id
from ambry.identity import GeneralNumber1
from . import Base, JSONAlchemy


class Config(Base):

    __tablename__ = 'config'
    __table_args__ = (
        UniqueConstraint('co_d_vid', 'co_type', 'co_group', 'co_key', name='_type_group_key_uc'),)

    id = SAColumn('co_id', String(32), primary_key=True)
    sequence_id = SAColumn('co_sequence_id', Integer, nullable=False, index=True)

    d_vid = SAColumn('co_d_vid', String(16), ForeignKey('datasets.d_vid'), index=True, doc='Dataset vid')
    type = SAColumn('co_type', String(200), doc='Type of the config: metadata, process, sync, etc...')
    group = SAColumn('co_group', String(200), doc='Group of the config: identity, about, etc...')
    key = SAColumn('co_key', String(200),  doc='Key of the config')
    value = SAColumn('co_value', JSONAlchemy(Text()),  doc='Value of the config key.')
    modified = SAColumn('co_modified', Integer(),
                        doc='Modification date: time in seconds since the epoch as a integer.')

    # Foreign key constraints may it hard to dump all of the configs to a new bundle database in
    # ambry.orm.database.Database#copy_dataset, so I've removed the foreign key constraint.
    # TODO: Write test for that note.

    parent_id = SAColumn(String(32), ForeignKey('config.co_id'), nullable=True,
                         doc='Id of the parent config.')

    parent = relationship('Config',  remote_side=[id])
    children = relationship('Config')

    def incver(self):
        """Increment all of the version numbers and return a new object"""
        from . import incver
        return incver(self, ['d_vid', 'id', 'parent_id'])

    @property
    def dict(self):
        return {p.key: getattr(self, p.key) for p in self.__mapper__.attrs}

    def __repr__(self):
        return u('<config: {},{},{} = {}>').format(self.d_vid, self.group, self.key, self.value)

    @property
    def dotted_key(self):
        return '{}.{}.{}'.format(self.type, self.group, self.key)

    def update_sequence_id(self, session, dataset):
        assert dataset.vid == self.d_vid
        assert session
        # NOTE: This next_sequence_id uses a different algorithm than dataset.next_sequence_id
        # FIXME replace this one with dataset.next_sequence_id
        self.sequence_id = next_sequence_id(session, dataset._sequence_ids, self.d_vid, Config)
        self.id = str(GeneralNumber1('F', self.d_vid, self.sequence_id))

    @staticmethod
    def before_insert(mapper, conn, target):

        if not target.sequence_id:
            from ambry.orm.exc import DatabaseError
            assert bool(target.d_vid)
            raise DatabaseError('Must set a sequence id before inserting')

        if not target.id:
            target.id = str(GeneralNumber1('F', target.d_vid, target.sequence_id))

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

        configs = self._session\
            .query(Config)\
            .filter_by(d_vid=self._dataset.vid,
                       type=self._type_name,
                       group=self._group_name)\
            .all()

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

                config.update_sequence_id(self._session, self._dataset)

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

    def delete_group(self, group):

        ssq = self._dataset.session.query
        ssq(Config).filter(Config.type == self._type_name).filter(Config.group == group).delete()

    def __iter__(self):
        for config in [config for config in self._dataset.configs if config.type == self._type_name]:
            yield config.dict

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


class BuildConfigGroupAccessor(ConfigGroupAccessor):
    """A config group acessor for the build group, which can calculate values and format times"""

    def __init__(self, dataset, type_name, session):
        super(BuildConfigGroupAccessor, self).__init__(dataset, type_name)
        self._session = session

    # FIXME! These functions should return sensible value when the underlying config items are missing
    # or have non-integer values

    def commit(self):
        self._session.commit()

    @property
    def build_duration(self):
        """Return the difference between build and build_done states"""

        return int(self.state.build_done) - int(self.state.build)

    @property
    def build_duration_pretty(self):
        """Return the difference between build and build_done states, in a human readable format"""
        from ambry.util import pretty_time
        from time import time

        if not self.state.building:
            return None

        built = self.state.built or time()

        try:
            return pretty_time(int(built) - int(self.state.building))
        except TypeError:  # one of the values is  None or not a number
            return None

    @property
    def built_datetime(self):
        """Return the built time as a datetime object"""
        from datetime import datetime
        try:
            return datetime.fromtimestamp(self.state.build_done)
        except TypeError:
            # build_done is null
            return None

    @property
    def new_datetime(self):
        """Return the time the bundle was created as a datetime object"""
        from datetime import datetime

        try:
            return datetime.fromtimestamp(self.state.new)
        except TypeError:
            return None

    @property
    def last_datetime(self):
        """Return the time of the last operation on the bundle as a datetime object"""
        from datetime import datetime

        try:
            return datetime.fromtimestamp(self.state.lasttime)
        except TypeError:
            return None
