# -*- coding: utf-8 -*-

import factory
from factory.alchemy import SQLAlchemyModelFactory

from ambry.orm import Dataset, Config, Table, Column, Partition, Code, ColumnStat, File


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
        sqlalchemy_session = None  # Set that just before DatasetFactory using.

    vid = factory.LazyAttribute(
        lambda dataset: '{self.id}{self.revision:03d}'.format(self=dataset))
    id = factory.Sequence(lambda n: 'dds%03d' % (n + 1,))

    name = factory.Sequence(lambda n: 'name-%03d' % n)
    vname = factory.Sequence(lambda n: 'vname-%03d' % n)
    fqname = factory.Sequence(lambda n: 'fqname-%03d' % n)
    cache_key = factory.Sequence(lambda n: 'cache-key-%03d' % n)
    source = factory.Sequence(lambda n: 'source-%03d' % n)
    dataset = factory.Sequence(lambda n: 'dataset-%03d' % n)
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
        assert dataset.id.startswith('d')
        assert dataset.revision > 0

        assert dataset.id in dataset.vid
        # Root config dataset vid should not have revision. I don't know why, see
        # ambry.library.database._add_config_root method.
        return True


class ConfigFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Config
        sqlalchemy_session = None  # Set that just before ConfigFactory using.

    d_vid = ''  # populates in the _prepare method.

    group = factory.Sequence(lambda n: '%03d' % n)
    key = factory.Sequence(lambda n: 'key-%03d' % n)
    value = factory.Sequence(lambda n: 'value-%03d' % n)
    sequence_id = factory.Iterator(list(range(1, 10)))

    @classmethod
    def _prepare(cls, create, **kwargs):
        d_vid = kwargs.get('d_vid', None)
        if d_vid:
            dataset = cls._meta.sqlalchemy_session\
                .query(Dataset)\
                .filter_by(vid=d_vid)\
                .one()
        else:
            if DatasetFactory._meta.sqlalchemy_session is None:
                DatasetFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
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

    id = ''  # will populate it in the _create method.
    vid = ''  # will populate it in the _create method.

    sequence_id = factory.Iterator(list(range(1, 10)))
    name = factory.Sequence(lambda n: 'name-%03d' % n)
    data = {}

    d_vid = ''  # Populates in the _create method.
    d_id = ''  # Populates in the _create method.

    @classmethod
    def _prepare(cls, create, **kwargs):
        dataset = kwargs.get('dataset', None)
        if not dataset:
            if DatasetFactory._meta.sqlalchemy_session is None:
                DatasetFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            dataset = DatasetFactory()

        kwargs['id'] = 't{dataset_id}{sequence_id:02d}'.format(
            dataset_id=_drop_entity(dataset.id),
            sequence_id=kwargs['sequence_id'])
        kwargs['vid'] = '{table_id}{dataset.revision:03d}'.format(
            table_id=kwargs['id'], dataset=dataset)

        kwargs['d_vid'] = dataset.vid
        kwargs['d_id'] = dataset.id
        kwargs['dataset'] = dataset
        instance = super(TableFactory, cls)._prepare(create, **kwargs)
        instance.set_attributes(**kwargs)
        return instance

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
        assert table.id.startswith('t')
        assert _drop_entity(table.id).startswith(_drop_entity(dataset.id))
        sequence_part = _drop_entity(table.id)\
            .replace(_drop_entity(dataset.id), '')
        assert sequence_part == ('%02d' % table.sequence_id)

        assert table.d_vid == dataset.vid
        return True


class ColumnFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Column
        sqlalchemy_session = None  # Set that just before TableFactory using.

    vid = ''  # Populates in the _prepare method.
    id = ''  # Populates in the _prepare method

    t_vid = ''  # Populates in the _prepare method.
    t_id = ''  # Populates in the _prepare method.

    d_vid = ''  # Populates in the _prepare method.

    sequence_id = factory.Iterator(list(range(1, 100)))

    @classmethod
    def _prepare(cls, create, **kwargs):
        table = kwargs.get('table', None)
        if not table:
            if TableFactory._meta.sqlalchemy_session is None:
                TableFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            table = TableFactory()

        kwargs['id'] = 'c{table_id}{sequence_id:03d}'.format(
            table_id=_drop_entity(table.id),
            sequence_id=kwargs['sequence_id'])
        kwargs['vid'] = '{column_id}{table_sequence_id:03d}'.format(
            column_id=kwargs['id'],
            table_sequence_id=table.sequence_id)

        kwargs['t_vid'] = table.vid
        kwargs['t_id'] = table.id
        kwargs['table'] = table

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
        table = cls._meta.sqlalchemy_session.query(Table).filter_by(vid=column.t_vid).first()
        assert column.vid.startswith('c')

        assert column.id.startswith('c')
        assert _drop_entity(column.id).startswith(_drop_entity(table.id))
        sequence_part = _drop_entity(column.id)\
            .replace(_drop_entity(table.id), '')
        assert sequence_part == '%03d' % column.sequence_id

        return True


class PartitionFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Partition
        sqlalchemy_session = None  # Set that just before TableFactory using.

    vid = ''  # populates in the _prepare method.
    id = ''  # populates in the _prepare method.

    name = factory.Sequence(lambda n: 'name-%03d' % n)
    vname = factory.Sequence(lambda n: 'vname-%03d' % n)
    fqname = factory.Sequence(lambda n: 'fqname-%03d' % n)
    format = 'db'
    space_coverage = []
    grain_coverage = []
    time_coverage = []

    t_vid = ''  # populates in the _prepare method
    d_vid = ''  # populates in the _prepare method.

    sequence_id = factory.Iterator(list(range(1, 100)))

    @classmethod
    def _prepare(cls, create, **kwargs):
        dataset = kwargs.get('dataset')
        if not dataset:
            DatasetFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            dataset = DatasetFactory()

        t_vid = kwargs.get('t_vid')
        if t_vid:
            table = cls._meta.sqlalchemy_session.query(Table).filter_by(vid=t_vid).one()
        else:
            TableFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            table = TableFactory(dataset=dataset)

        kwargs['dataset'] = dataset
        kwargs['d_vid'] = dataset.vid
        kwargs['t_vid'] = table.vid

        kwargs['id'] = 'p{dataset_id}{sequence_id:03d}'.format(
            dataset_id=_drop_entity(dataset.id),
            sequence_id=kwargs['sequence_id'])

        kwargs['vid'] = '{id}{dataset.revision:03d}'.format(
            id=kwargs['id'],
            dataset=dataset)

        instance = super(PartitionFactory, cls)._prepare(create, **kwargs)
        return instance

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

        # .id validation
        assert partition.id.startswith('p')
        assert _drop_entity(partition.id).startswith(_drop_entity(dataset.id))
        sequence_part = _drop_entity(partition.id)\
            .replace(_drop_entity(dataset.id), '')
        assert sequence_part == ('%03d' % partition.sequence_id)

        # .vid validation
        assert partition.vid.startswith('p')
        assert partition.vid.startswith(partition.id)
        dataset_revision_part = partition.vid.replace(partition.id, '')
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
        # shortcut for sqlalchemy query.
        query = cls._meta.sqlalchemy_session.query

        d_vid = kwargs.get('d_vid', None)
        if d_vid:
            dataset = query(Dataset).filter_by(vid=d_vid).one()
        else:
            if DatasetFactory._meta.sqlalchemy_session is None:
                DatasetFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            dataset = DatasetFactory()

        p_vid = kwargs.get('p_vid', None)
        if d_vid:
            partition = query(Partition).filter_by(vid=p_vid).one()
        else:
            if PartitionFactory._meta.sqlalchemy_session is None:
                PartitionFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            partition = PartitionFactory(dataset=dataset)

        c_vid = kwargs.get('c_vid', None)
        if d_vid:
            column = query(Column).filter_by(vid=c_vid).one()
        else:
            if TableFactory._meta.sqlalchemy_session is None:
                TableFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            table = TableFactory(dataset=dataset)
            if ColumnFactory._meta.sqlalchemy_session is None:
                ColumnFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
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


class FileFactory(SQLAlchemyModelFactory):
    class Meta:
        model = File
        sqlalchemy_session = None  # Set that just before TableFactory using.

    id = factory.Sequence(lambda n: n)
    d_vid = ''  # populates in the _prepare method.

    path = factory.Sequence(lambda n: 'path-%03d' % n)
    major_type = factory.Sequence(lambda n: 'major_type-%03d' % n)
    minor_type = factory.Sequence(lambda n: 'minor_type-%03d' % n)
    source = factory.Sequence(lambda n: 'source-%03d' % n)

    @classmethod
    def _prepare(cls, create, **kwargs):
        dataset = kwargs.get('dataset')
        if not dataset:
            DatasetFactory._meta.sqlalchemy_session = cls._meta.sqlalchemy_session
            dataset = DatasetFactory()

        kwargs['dataset'] = dataset
        kwargs['d_vid'] = dataset.vid
        instance = super(FileFactory, cls)._prepare(create, **kwargs)
        instance.set_attributes(**kwargs)
        return instance
