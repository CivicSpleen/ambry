# -*- coding: utf-8 -*-
import os

import factory
from factory.alchemy import SQLAlchemyModelFactory

from ambry.library.database import LibraryDb
from ambry.library import Library
from ambry.library.files import Files
from ambry.warehouse import Warehouse
from ambry.orm import Dataset, Config, Table, Column, File, Partition, Code, ColumnStat
from ambry.library.database import ROOT_CONFIG_NAME

WAREHOUSE_LIBRARY_DB_FILE = 'warehouse-lib-db.db'
WAREHOUSE_REMOTE_LIBRARY_DB_FILE = 'warehouse-remote-lib-db.db'

# TODO: move orm factories to the orm tests dir


def _drop_entity(vid):
    ''' Removes leads entity flag from the vid. '''
    DATASET = 'd'
    PARTITION = 'p'
    TABLE = 't'
    COLUMN = 'c'
    COLUMN_STAT = 'cs'
    ENTITIES = [
        DATASET, PARTITION,
        TABLE, COLUMN,
        COLUMN_STAT]
    assert vid[0] in ENTITIES
    return vid[1:]


class DatasetFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Dataset
        sqlalchemy_session = None  # Set that just before ConfigFactory using.

    vid = factory.LazyAttribute(
        lambda dataset: '{self.id_}{self.revision:03d}'.format(self=dataset))
    id_ = factory.Sequence(lambda n: 'dds%03d' % (n + 1,))

    name = factory.Sequence(lambda n: 'name-%03d' % n)
    vname = factory.Sequence(lambda n: 'vname-%03d' % n)
    fqname = factory.Sequence(lambda n: 'fqname-%03d' % n)
    cache_key = factory.Sequence(lambda n: 'cache-key-%03d' % n)
    source = factory.Sequence(lambda n: 'source-%03d' % n)
    dataset = factory.Sequence(lambda n: 'dataset-%03d' % n)
    creator = factory.Sequence(lambda n: 'creator-%03d' % n)
    revision = 1
    version = '0.1.3'

    @classmethod
    def _after_postgeneration(cls, dataset, create, results=None):
        cls.validate(dataset)

    @classmethod
    def validate(cls, dataset):
        """Returns True if fields populated with correct values. Otherwise raises AssertionError.

        Args:
            dataset (Dataset): dataset instance to validate.
        """
        assert dataset.vid.startswith('d')
        assert dataset.id_.startswith('d')
        assert dataset.revision > 0

        assert dataset.id_ in dataset.vid
        # Root config dataset vid should not have revision. I don't know why, see
        # ambry.library.database._add_config_root method.
        if not dataset.id_ == ROOT_CONFIG_NAME:
            assert dataset.vid.replace(dataset.id_, '') == ('%03d' % dataset.revision)
        return True


class ConfigFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Config
        sqlalchemy_session = None  # Set that just before ConfigFactory using.

    d_vid = ''  # populates in the _prepare method.

    group = factory.Sequence(lambda n: '%03d' % n)
    key = factory.Sequence(lambda n: 'key-%03d' % n)
    value = factory.Sequence(lambda n: 'value-%03d' % n)

    @classmethod
    def _prepare(cls, create, **kwargs):
        d_vid = kwargs.get('d_vid', None)
        if d_vid:
            dataset = cls._meta.sqlalchemy_session\
                .query(Dataset)\
                .filter_by(vid=d_vid)\
                .one()
        else:
            dataset = DatasetFactory()

        kwargs['d_vid'] = dataset.vid
        return super(ConfigFactory, cls)._prepare(create, **kwargs)

    @classmethod
    def _after_postgeneration(cls, config, create, results=None):
        cls.validate(config)

    @classmethod
    def validate(cls, config):
        """Returns True if fields populated with correct values. Otherwise raises AssertionError.

        Args:
            config (Config): config instance to validate.
        """
        assert config.d_vid.startswith('d')
        dataset = cls._meta.sqlalchemy_session.query(Dataset).filter_by(vid=config.d_vid).one()
        assert dataset
        assert DatasetFactory.validate(dataset)
        return True


class TableFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Table
        sqlalchemy_session = None  # Set that just before TableFactory using.

    id_ = ''  # will populate it in the _create method.
    vid = ''  # will populate it in the _create method.

    sequence_id = factory.Iterator(range(1, 10))
    name = factory.Sequence(lambda n: 'name-%03d' % n)
    data = {}

    d_vid = ''  # Populates in the _create method.
    d_id = ''  # Populates in the _create method.

    @classmethod
    def _prepare(cls, create, **kwargs):
        dataset = kwargs.get('dataset', None)
        if not dataset:
            dataset = DatasetFactory()

        kwargs['id_'] = 't{dataset_id}{sequence_id:02d}'.format(
            dataset_id=_drop_entity(dataset.id_),
            sequence_id=kwargs['sequence_id'])
        kwargs['vid'] = '{table_id}{dataset.revision:02d}'.format(
            table_id=kwargs['id_'], dataset=dataset)

        kwargs['d_vid'] = dataset.vid
        kwargs['d_id'] = dataset.id_
        return super(TableFactory, cls)._prepare(create, **kwargs)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        dataset = kwargs.pop('dataset')
        args += (dataset,)
        return super(TableFactory, cls)._create(model_class, *args, **kwargs)

    @classmethod
    def _after_postgeneration(cls, table, create, results=None):
        cls.validate(table)

    @classmethod
    def validate(cls, table):
        """Returns True if fields populated with correct values. Otherwise raises AssertionError.

        Args:
            table (Table): table instance to validate.
        """
        assert table.vid.startswith('t')

        dataset = cls._meta.sqlalchemy_session.query(Dataset).filter_by(vid=table.d_vid).one()
        assert table.id_.startswith('t')
        assert _drop_entity(table.id_).startswith(_drop_entity(dataset.id_))
        sequence_part = _drop_entity(table.id_)\
            .replace(_drop_entity(dataset.id_), '')
        assert sequence_part == ('%02d' % table.sequence_id)

        assert table.d_vid == dataset.vid
        return True


class ColumnFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Column
        sqlalchemy_session = None  # Set that just before TableFactory using.

    vid = ''  # Populates in the _prepare method.
    id_ = ''  # Populates in the _prepare method

    t_vid = ''  # Populates in the _prepare method.
    t_id = ''  # Populates in the _prepare method.

    d_vid = ''  # Populates in the _prepare method.

    sequence_id = factory.Iterator(range(1, 100))

    @classmethod
    def _prepare(cls, create, **kwargs):
        table = kwargs.get('table', None)
        if not table:
            table = TableFactory()

        kwargs['id_'] = 'c{table_id}{sequence_id:03d}'.format(
            table_id=_drop_entity(table.id_),
            sequence_id=kwargs['sequence_id'])
        kwargs['vid'] = 'c{column_id}{table_sequence_id:03d}'.format(
            column_id=kwargs['id_'],
            table_sequence_id=table.sequence_id)

        kwargs['t_vid'] = table.vid
        kwargs['t_id'] = table.id_

        dataset = cls._meta.sqlalchemy_session.query(Dataset).filter_by(vid=table.d_vid).one()
        kwargs['d_vid'] = dataset.vid

        return super(ColumnFactory, cls)._prepare(create, **kwargs)

    @classmethod
    def _after_postgeneration(cls, column, create, results=None):
        cls.validate(column)

    @classmethod
    def validate(cls, column):
        """Returns True if fields populated with correct values. Otherwise raises AssertionError.

        Args:
            column (Column): column instance to validate.
        """
        # dataset = cls._meta.sqlalchemy_session.query(Dataset).filter_by(vid=column.d_vid).one()
        table = cls._meta.sqlalchemy_session.query(Table).filter_by(vid=column.t_vid).one()
        assert column.vid.startswith('c')

        assert column.id_.startswith('c')
        assert _drop_entity(column.id_).startswith(_drop_entity(table.id_))
        sequence_part = _drop_entity(column.id_)\
            .replace(_drop_entity(table.id_), '')
        assert sequence_part == '%03d' % column.sequence_id

        return True


class FileFactory(SQLAlchemyModelFactory):
    class Meta:
        model = File
        sqlalchemy_session = None  # Set that just before TableFactory using.

    oid = factory.Sequence(lambda n: n)
    path = factory.Sequence(lambda n: 'path-%03d' % n)
    type_ = Files.TYPE.BUNDLE
    source_url = 'http://example.com'


class PartitionFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Partition
        sqlalchemy_session = None  # Set that just before TableFactory using.

    vid = ''  # populates in the _prepare method.
    id_ = ''  # populates in the _prepare method.

    name = factory.Sequence(lambda n: 'name-%03d' % n)
    vname = factory.Sequence(lambda n: 'vname-%03d' % n)
    fqname = factory.Sequence(lambda n: 'fqname-%03d' % n)
    format = 'db'
    cache_key = ''  # populates in the _prepare method

    t_id = ''  # populates in the _prepare method
    d_vid = ''  # populates in the _prepare method.
    d_id = ''  # populates in the _prepare method.

    sequence_id = factory.Iterator(range(1, 100))

    @classmethod
    def _prepare(cls, create, **kwargs):
        dataset = kwargs.get('dataset')
        if not dataset:
            DatasetFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            dataset = DatasetFactory()

        t_id = kwargs.get('t_id')
        if t_id:
            table = cls._meta.sqlalchemy_session.query(Table).filter_by(id_=t_id).one()
        else:
            TableFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            table = TableFactory(dataset=dataset)

        kwargs['dataset'] = dataset
        kwargs['t_id'] = table.id_

        kwargs['id_'] = 'p{dataset_id}{sequence_id:03d}'.format(
            dataset_id=_drop_entity(dataset.id_),
            sequence_id=kwargs['sequence_id'])

        kwargs['vid'] = '{id_}{dataset.revision:03d}'.format(
            id_=kwargs['id_'],
            dataset=dataset)
        kwargs['cache_key'] = '%s-key.%s' % (kwargs['vid'], kwargs['format'])

        kwargs['d_vid'] = dataset.vid
        kwargs['d_id'] = dataset.id_

        return super(PartitionFactory, cls)._prepare(create, **kwargs)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        dataset = kwargs.pop('dataset')
        t_id = kwargs.pop('t_id')
        args += (dataset, t_id)
        return super(PartitionFactory, cls)._create(model_class, *args, **kwargs)

    @classmethod
    def _after_postgeneration(cls, partition, create, results=None):
        cls.validate(partition)

    @classmethod
    def validate(cls, partition):
        """Returns True if fields populated with correct values. Otherwise raises AssertionError.

        Args:
            partition (Partition): partition instance to validate.
        """
        dataset = cls._meta.sqlalchemy_session.query(Dataset).filter_by(vid=partition.d_vid).one()

        # .id_ validation
        assert partition.id_.startswith('p')
        assert _drop_entity(partition.id_).startswith(_drop_entity(dataset.id_))
        sequence_part = _drop_entity(partition.id_)\
            .replace(_drop_entity(dataset.id_), '')
        assert sequence_part == ('%03d' % partition.sequence_id)

        # .vid validation
        assert partition.vid.startswith('p')
        assert partition.vid.startswith(partition.id_)
        dataset_revision_part = partition.vid.replace(partition.id_, '')
        assert dataset_revision_part == ('%03d' % dataset.revision)
        return True


class CodeFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Code
        sqlalchemy_session = None  # Set that just before CodeFactory using.
    # TODO: uncomment and implement.
    '''
    oid = factory.Sequence(lambda n: n)
    key = factory.Sequence(lambda n: 'key-%03d' % n)
    value = factory.Sequence(lambda n: 'value-%03d' % n)
    '''


class ColumnStatFactory(SQLAlchemyModelFactory):
    class Meta:
        model = ColumnStat
        sqlalchemy_session = None  # Set that just before ColumnStatFactory using.

    p_vid = ''  # populates in the _prepare method.
    c_vid = ''  # populates in the _prepare method.
    d_vid = ''  # populates in the _prepare method.

    @classmethod
    def _prepare(cls, create, **kwargs):
        # shortcut for sqlalchemy session.
        query = cls._meta.sqlalchemy_session.query

        d_vid = kwargs.get('d_vid', None)
        if d_vid:
            dataset = query(Dataset).filter_by(vid=d_vid).one()
        else:
            dataset = DatasetFactory()

        p_vid = kwargs.get('p_vid', None)
        if d_vid:
            partition = query(Partition).filter_by(vid=p_vid).one()
        else:
            partition = PartitionFactory(dataset=dataset)

        c_vid = kwargs.get('c_vid', None)
        if d_vid:
            column = query(Column).filter_by(vid=c_vid).one()
        else:
            table = TableFactory(dataset=dataset)
            column = ColumnFactory(table=table)

        kwargs['d_vid'] = dataset.vid
        kwargs['p_vid'] = partition.vid
        kwargs['c_vid'] = column.vid

        return super(ColumnStatFactory, cls)._prepare(create, **kwargs)

    @classmethod
    def _after_postgeneration(cls, column_stat, create, results=None):
        cls.validate(column_stat)

    @classmethod
    def validate(cls, column_stat):
        """Returns True if fields populated with correct values. Otherwise raises AssertionError.

        Args:
            column_stat (ColumnStat): column stat instance to validate.
        """
        # shortcut for sqlalchemy session.
        query = cls._meta.sqlalchemy_session.query

        dataset = query(Dataset).filter_by(vid=column_stat.d_vid).one()
        partition = query(Partition).filter_by(vid=column_stat.p_vid).one()
        column = query(Column).filter_by(vid=column_stat.c_vid).one()
        assert partition.d_vid == column.d_vid
        assert partition.d_vid == dataset.vid

        return True


class WarehouseFactory(factory.Factory):
    # TODO: try to upgrade sqlite to use multiple inmemory files\
    # - http://www.sqlite.org/changes.html#version_3_7_13
    _databases_to_close = []

    class Meta:
        model = Warehouse

    @classmethod
    def _prepare(cls, create, **kwargs):
        # shortcut for sqlalchemy session.
        assert 'database' in kwargs, 'Database is required for Warehouse.'

        cache = {'key1': 'val1'}  # TODO: convert to valid cache
        if not kwargs.get('wlibrary'):
            wlibrary_db = LibraryDb(
                driver='sqlite',
                dbname=WAREHOUSE_LIBRARY_DB_FILE)
            wlibrary_db.enable_delete = True
            wlibrary_db.create_tables()
            wlibrary = Library(cache, wlibrary_db)
            kwargs['wlibrary'] = wlibrary
            cls._databases_to_close.append(wlibrary_db)

        if not kwargs.get('elibrary'):
            elibrary_db = LibraryDb(
                driver='sqlite',
                dbname=WAREHOUSE_REMOTE_LIBRARY_DB_FILE)
            elibrary_db.enable_delete = True
            elibrary_db.create_tables()
            elibrary = Library(cache, elibrary_db)
            kwargs['elibrary'] = elibrary
            cls._databases_to_close.append(wlibrary_db)

        return super(WarehouseFactory, cls)._prepare(create, **kwargs)

    @classmethod
    def clean(cls):
        """ deletes all filed created while constructing warehouse. """
        for db in cls._databases_to_close:
            db.close()
        try:
            os.remove(WAREHOUSE_LIBRARY_DB_FILE)
        except OSError:
            pass

        try:
            os.remove(WAREHOUSE_REMOTE_LIBRARY_DB_FILE)
        except OSError:
            pass
