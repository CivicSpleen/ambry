# -*- coding: utf-8 -*-
import unittest

import unicodecsv

try:
    # py2, mock is external lib.
    from mock import patch, MagicMock
except ImportError:
    # py3, mock is included
    from unittest.mock import patch, MagicMock

import six

from ambry.exporters.ckan.core import _convert_bundle, _convert_partition, export, MISSING_CREDENTIALS_MSG,\
    UnpublishedAccessError
from ambry.orm.database import Database
from ambry.run import get_runconfig

from test.unit.orm_factories import DatasetFactory, PartitionFactory, FileFactory


class ConvertDatasetTest(unittest.TestCase):
    def setUp(self):
        self.sqlite_db = Database('sqlite://')
        self.sqlite_db.create()

    def test_converts_bundle_to_dict(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        bundle = _get_fake_bundle(ds1)
        self.sqlite_db.commit()
        ret = _convert_bundle(bundle)
        self.assertIn('name', ret)
        self.assertIsNotNone(ret['name'])
        self.assertEqual(ret['name'], ds1.vid)

        self.assertIn('title', ret)
        self.assertIsNotNone(ret['title'])
        self.assertEqual(ret['title'], ds1.config.metadata.about.title)

        self.assertIn('author', ret)
        self.assertIn('author_email', ret)
        self.assertIn('maintainer', ret)
        self.assertIn('maintainer_email', ret)

    def test_extends_notes_with_dataset_documentation(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        FileFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        FileFactory(dataset=ds1, path='documentation.md', contents='### Dataset documentation.')
        self.sqlite_db.commit()
        bundle = _get_fake_bundle(ds1)
        ret = _convert_bundle(bundle)

        self.assertIn('### Dataset documentation.', ret['notes'])


class ConvertPartitionTest(unittest.TestCase):

    def setUp(self):
        self.sqlite_db = Database('sqlite://')
        self.sqlite_db.create()

    def test_converts_partition_to_resource_dict(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        partition1 = PartitionFactory(dataset=ds1)
        self.sqlite_db.commit()
        partition1._datafile = MagicMock()
        ret = _convert_partition(partition1)
        self.assertIn('package_id', ret)
        self.assertEqual(ret['package_id'], ds1.vid)
        self.assertEqual(ret['name'], partition1.name)

    def test_converts_partition_content_to_csv(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        partition1 = PartitionFactory(dataset=ds1)
        self.sqlite_db.commit()

        # make partition iterator return my values.
        partition1._datafile = MagicMock()
        partition1._datafile.headers = ['col1', 'col2']
        fake_iter = lambda: iter([{'col1': '1', 'col2': '1'}, {'col1': '2', 'col2': '2'}])
        with patch('ambry.orm.partition.Partition.__iter__', side_effect=fake_iter):
            ret = _convert_partition(partition1)

        # check converted partition.
        self.assertIn('package_id', ret)
        self.assertEqual(ret['package_id'], ds1.vid)
        self.assertIn('upload', ret)
        self.assertTrue(isinstance(ret['upload'], six.StringIO))
        rows = []
        reader = unicodecsv.reader(ret['upload'])
        for row in reader:
            rows.append(row)
        self.assertEqual(rows[0], ['col1', 'col2'])
        self.assertEqual(rows[1], ['1', '1'])
        self.assertEqual(rows[2], ['2', '2'])


class ExportTest(unittest.TestCase):
    """ Tests export(bundle) function. """

    def setUp(self):
        rc = get_runconfig()
        if 'ckan' not in rc.accounts:
            raise EnvironmentError(MISSING_CREDENTIALS_MSG)
        self.sqlite_db = Database('sqlite://')
        self.sqlite_db.create()

    @patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action')
    def test_creates_package_for_given_dataset(self, fake_call):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        ds1 = DatasetFactory()
        ds1.config.metadata.about.access = 'public'
        bundle = _get_fake_bundle(ds1)
        export(bundle)

        # assert call to service was valid.
        called = False
        for call in fake_call.mock_calls:
            _, args, kwargs = call
            if (args[0] == 'package_create'
                    and kwargs['data_dict'].get('name') == ds1.vid):
                called = True
        self.assertTrue(called)

    @patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action')
    @patch('ambry.exporters.ckan.core._convert_partition')
    def test_creates_resource_for_each_partition_of_the_bundle(self, fake_convert, fake_call):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        ds1.config.metadata.about.access = 'public'
        p1 = PartitionFactory(dataset=ds1)
        bundle = _get_fake_bundle(ds1, partitions=[p1])
        fake_convert.return_value = {'name': p1.name, 'package_id': ds1.vid}
        export(bundle)

        # assert call to service was valid.
        called = False
        for call in fake_call.mock_calls:
            _, args, kwargs = call
            if (args[0] == 'resource_create'
                    and kwargs['data_dict'].get('name') == p1.name
                    and kwargs['data_dict'].get('package_id') == ds1.vid):
                called = True
        self.assertTrue(called)

    @patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action')
    @patch('ambry.exporters.ckan.core._convert_schema')
    def test_creates_resource_for_schema(self, fake_convert, fake_call):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        ds1.config.metadata.about.access = 'public'
        bundle = _get_fake_bundle(ds1)
        fake_convert.return_value = {'name': 'schema', 'package_id': ds1.vid}
        export(bundle)

        # assert call to service was valid.
        called = False
        for call in fake_call.mock_calls:
            _, args, kwargs = call
            if (args[0] == 'resource_create'
                    and kwargs['data_dict'].get('name') == 'schema'
                    and kwargs['data_dict'].get('package_id') == ds1.vid):
                called = True
        self.assertTrue(called)

    @patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action')
    def test_creates_resource_for_each_external_documentation(self, fake_call):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        ds1.config.metadata.about.access = 'public'

        # create two external documentations.
        #
        site1_descr = 'Descr1'
        site1_url = 'http://example.com/1'
        site2_descr = 'Descr2'
        site2_url = 'http://example.com/2'

        ds1.config.metadata.external_documentation.site1.description = site1_descr
        ds1.config.metadata.external_documentation.site1.url = site1_url

        ds1.config.metadata.external_documentation.site2.description = site2_descr
        ds1.config.metadata.external_documentation.site2.url = site2_url

        bundle = _get_fake_bundle(ds1)
        export(bundle)

        # assert call was valid
        resource_create_calls = {}
        for call in fake_call.mock_calls:
            _, args, kwargs = call
            if args[0] == 'resource_create':
                resource_create_calls[kwargs['data_dict']['name']] = kwargs['data_dict']
        self.assertIn('site1', resource_create_calls)
        self.assertEqual(resource_create_calls['site1']['url'], site1_url)
        self.assertEqual(resource_create_calls['site1']['description'], site1_descr)

        self.assertIn('site2', resource_create_calls)
        self.assertEqual(resource_create_calls['site2']['url'], site2_url)
        self.assertEqual(resource_create_calls['site2']['description'], site2_descr)

    @patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action')
    def test_raises_UnpublishedAccessError_error(self, fake_call):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        ds1.config.metadata.about.access = 'restricted'
        bundle = _get_fake_bundle(ds1)
        with self.assertRaises(UnpublishedAccessError):
            export(bundle)


class ConvertSchemaTest(unittest.TestCase):
    """ tests _convert_schema function. """

    def setUp(self):
        self.sqlite_db = Database('sqlite://')
        self.sqlite_db.create()

    def _test_converts_schema_to_resource_dict(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        FileFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        partition1 = PartitionFactory(dataset=ds1)
        self.sqlite_db.commit()
        partition1._datafile = MagicMock()
        ret = _convert_partition(partition1)
        self.assertIn('package_id', ret)
        self.assertEqual(ret['package_id'], ds1.vid)
        self.assertEqual(ret['name'], partition1.name)

    def test_converts_partition_content_to_csv(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        partition1 = PartitionFactory(dataset=ds1)
        self.sqlite_db.commit()

        # make partition iterator return my values.
        partition1._datafile = MagicMock()
        partition1._datafile.headers = ['col1', 'col2']
        fake_iter = lambda: iter([{'col1': '1', 'col2': '1'}, {'col1': '2', 'col2': '2'}])
        with patch('ambry.orm.partition.Partition.__iter__', side_effect=fake_iter):
            ret = _convert_partition(partition1)

        # check converted partition.
        self.assertIn('package_id', ret)
        self.assertEqual(ret['package_id'], ds1.vid)
        self.assertIn('upload', ret)
        self.assertTrue(isinstance(ret['upload'], six.StringIO))
        rows = []
        reader = unicodecsv.reader(ret['upload'])
        for row in reader:
            rows.append(row)
        self.assertEqual(rows[0], ['col1', 'col2'])
        self.assertEqual(rows[1], ['1', '1'])
        self.assertEqual(rows[2], ['2', '2'])


def _get_fake_bundle(dataset, partitions=None):
    """ Returns bundle like object. """
    bundle = MagicMock()
    bundle.dataset = dataset
    bundle.partitions = partitions or []
    return bundle
