# -*- coding: utf-8 -*-

import factory
from factory.alchemy import SQLAlchemyModelFactory

from ambry.orm import Dataset, Config, Table, Column, File, Partition, Code, ColumnStat

# TODO: move to the orm tests dir


class DatasetFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Dataset
        sqlalchemy_session = None  # Set that just before ConfigFactory using.

    vid = factory.Sequence(lambda n: '%03d' % n)
    name = factory.Sequence(lambda n: 'name-%03d' % n)
    vname = factory.Sequence(lambda n: 'vname-%03d' % n)
    fqname = factory.Sequence(lambda n: 'fqname-%03d' % n)
    cache_key = factory.Sequence(lambda n: 'cache_key-%03d' % n)
    source = factory.Sequence(lambda n: 'source-%03d' % n)
    dataset = factory.Sequence(lambda n: 'dataset-%03d' % n)
    creator = factory.Sequence(lambda n: 'creator-%03d' % n)
    revision = 1
    version = '0.1'


class ConfigFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Config
        sqlalchemy_session = None  # Set that just before ConfigFactory using.

    d_vid = factory.Sequence(lambda n: '%03d' % n)
    group = factory.Sequence(lambda n: '%03d' % n)
    key = factory.Sequence(lambda n: 'key-%03d' % n)
    value = factory.Sequence(lambda n: 'value-%03d' % n)


class TableFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Table
        sqlalchemy_session = None  # Set that just before TableFactory using.

    vid = factory.Sequence(lambda n: '%03d' % n)
    sequence_id = 1
    name = factory.Sequence(lambda n: 'name-%03d' % n)
    data = {}


class ColumnFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Column
        sqlalchemy_session = None  # Set that just before TableFactory using.

    vid = factory.Sequence(lambda n: '%03d' % n)


class FileFactory(SQLAlchemyModelFactory):
    class Meta:
        model = File
        sqlalchemy_session = None  # Set that just before TableFactory using.

    oid = factory.Sequence(lambda n: n)
    path = factory.Sequence(lambda n: 'path-%03d' % n)


class PartitionFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Partition
        sqlalchemy_session = None  # Set that just before TableFactory using.

    vid = factory.Sequence(lambda n: 'vid-%03d' % n)
    id_ = factory.Sequence(lambda n: 'id_-%03d' % n)
    name = factory.Sequence(lambda n: 'name-%03d' % n)
    vname = factory.Sequence(lambda n: 'vname-%03d' % n)
    fqname = factory.Sequence(lambda n: 'fqname-%03d' % n)
    format = 'csv'
    cache_key = factory.Sequence(lambda n: 'cache_key-%03d.csv' % n)


class CodeFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Code
        sqlalchemy_session = None  # Set that just before CodeFactory using.
    oid = factory.Sequence(lambda n: n)
    key = factory.Sequence(lambda n: 'key-%03d' % n)
    value = factory.Sequence(lambda n: 'value-%03d' % n)


class ColumnStatFactory(SQLAlchemyModelFactory):
    class Meta:
        model = ColumnStat
        sqlalchemy_session = None  # Set that just before TableFactory using.

    id = factory.Sequence(lambda n: n)
