# -*- coding: utf-8 -*-
import unittest

from mock import patch

from ambry.exporters.ckan.core import _convert_dataset, _convert_partition, export, MISSING_CREDENTIALS_MSG,\
    UnpublishedAccessError
from ambry.orm.database import Database
from ambry.run import get_runconfig

from test.test_orm.factories import DatasetFactory, PartitionFactory, TableFactory, FileFactory


class ConvertDatasetTest(unittest.TestCase):
    def setUp(self):
        self.sqlite_db = Database('sqlite://')
        self.sqlite_db.create()

    def test_converts_dataset_to_dict(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        self.sqlite_db.commit()
        ret = _convert_dataset(ds1)
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
        ret = _convert_dataset(ds1)

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
        ret = _convert_partition(partition1)
        self.assertIn('package_id', ret)
        self.assertEqual(ret['package_id'], ds1.vid)


class ExportTest(unittest.TestCase):
    """ Tests export(dataset) function. """

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
        export(ds1)

        # assert call to service was valid.
        self.assertTrue(len(fake_call.mock_calls) > 0)
        _, args, kwargs = fake_call.mock_calls[0]
        self.assertEqual(args[0], 'package_create')
        self.assertEqual(kwargs['data_dict']['name'], ds1.vid)

    @patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action')
    def test_creates_resources_for_each_partition_of_the_dataset(self, fake_call):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        ds1.config.metadata.about.access = 'public'
        PartitionFactory(dataset=ds1)
        export(ds1)

        # assert call to service was valid.
        self.assertTrue(len(fake_call.mock_calls) > 4)
        _, args, kwargs = fake_call.mock_calls[3]
        self.assertEqual(args[0], 'resource_create')
        self.assertEqual(kwargs['data_dict']['package_id'], ds1.vid)

    @patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action')
    def test_creates_resource_for_schema(self, fake_call):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        ds1.config.metadata.about.access = 'public'
        TableFactory(dataset=ds1)
        export(ds1)

        # assert call to service was valid.
        self.assertTrue(len(fake_call.mock_calls) > 3)
        _, args, kwargs = fake_call.mock_calls[3]
        self.assertEqual(args[0], 'resource_create')
        self.assertEqual(kwargs['data_dict']['package_id'], ds1.vid)
        self.assertEqual(kwargs['data_dict']['name'], 'schema')

    @patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action')
    def test_raises_UnpublishedAccessError_error(self, fake_call):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        TableFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory()
        ds1.config.metadata.about.access = 'restricted'
        with self.assertRaises(UnpublishedAccessError):
            export(ds1)
