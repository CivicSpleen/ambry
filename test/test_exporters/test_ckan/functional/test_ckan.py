# -*- coding: utf-8 -*-
import unittest

from mock import patch

from ambry.orm.database import Database
from ambry.run import get_runconfig
from ambry.exporters.ckan.core import export, MISSING_CREDENTIALS_MSG

from test.test_orm.factories import DatasetFactory, PartitionFactory, FileFactory

# CKAN is mocked by default. If you really want to hit CKAN instance set MOCK_CKAN to False.
MOCK_CKAN = True


class Test(unittest.TestCase):

    def setUp(self):
        rc = get_runconfig()
        if 'ckan' not in rc.accounts:
            raise EnvironmentError(MISSING_CREDENTIALS_MSG)
        self.sqlite_db = Database('sqlite://')
        self.sqlite_db.create()
        self._requests = {}  # calls to CKAN mock.

    def test_dataset_export(self):
        DatasetFactory._meta.sqlalchemy_session = self.sqlite_db.session
        PartitionFactory._meta.sqlalchemy_session = self.sqlite_db.session
        FileFactory._meta.sqlalchemy_session = self.sqlite_db.session

        ds1 = DatasetFactory(vid='dds0011')
        ds1.config.metadata.about.access = 'public'
        ds1.config.metadata.contacts.creator.name = 'creator'
        ds1.config.metadata.contacts.creator.name = 'creator'
        ds1.config.metadata.contacts.creator.email = 'creator@example.com'
        ds1.config.metadata.contacts.maintainer.name = 'maintainer'
        ds1.config.metadata.contacts.maintainer.email = 'maintainer@example.com'

        PartitionFactory(dataset=ds1)
        FileFactory(id=20, dataset=ds1, path='schema.csv', contents='table,datatype,size,column,description')
        FileFactory(id=21, dataset=ds1, path='documentation.md', contents='### Documentation')

        self.sqlite_db.commit()

        if MOCK_CKAN:
            def call_replacement(action, **kwargs):
                if action not in self._requests:
                    self._requests[action] = []
                self._requests[action].append(kwargs['data_dict'])

            with patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action', side_effect=call_replacement):
                export(ds1)
                self._assert_dataset_published(ds1)
                self._assert_schema_published(ds1)
                self._assert_partitions_published(ds1)
        else:
            # Real requests to CKAN instance.
            export(ds1)

            # retrieve saved document from remote CKAN
            from ambry.exporters.ckan.core import ckan
            params = {'q': 'name:{}'.format(ds1.vid)}
            resp = ckan.action.package_search(**params)
            self.assertEqual(len(resp['results']), 1)
            dataset_package = resp['results'][0]

            self._assert_dataset_published(ds1, dataset_package=dataset_package)
            self._assert_schema_published(ds1, dataset_package=dataset_package)
            self._assert_partitions_published(ds1, dataset_package=dataset_package)

    def _assert_dataset_published(self, dataset, dataset_package=None):
        if MOCK_CKAN:
            self.assertIn('package_create', self._requests)
            self.assertEqual(len(self._requests['package_create']), 1)
            dataset_package = self._requests['package_create'][0]
        else:
            assert dataset_package

        self.assertEqual(dataset_package['name'], dataset.vid)
        self.assertIn('### Documentation', dataset_package['notes'])

        # test contacts.
        contacts = dataset.config.metadata.contacts
        self.assertEqual(dataset_package['author'], contacts.creator.name)
        self.assertEqual(dataset_package['author_email'], contacts.creator.email)

        self.assertEqual(dataset_package['maintainer'], contacts.maintainer.name)
        self.assertEqual(dataset_package['maintainer_email'], contacts.maintainer.email)

    def _assert_schema_published(self, dataset, dataset_package=None):
        # test schema resource and schema.csv content.

        if MOCK_CKAN:
            self.assertIn('resource_create', self._requests)
            schema_resource = [x for x in self._requests['resource_create'] if x['name'] == 'schema']
            assert schema_resource
            schema_resource = schema_resource[0]
            self.assertEqual(schema_resource['upload'], 'table,datatype,size,column,description')
        else:
            assert dataset_package
            schema_resources = [x for x in dataset_package['resources'] if x['name'] == 'schema']
            assert len(schema_resources) == 1
            schema_resource = schema_resources[0]
            # FIXME: Read content of the file.

        # self.assertEqual(schema_resource['package_id'], dataset.vid)

    def _assert_partitions_published(self, dataset, dataset_package=None):
        # check first partition only.
        # TODO: check more than one partition.
        partition = dataset.partitions[0]
        if MOCK_CKAN:
            partition_resource = [x for x in self._requests['resource_create'] if x['name'] == partition.name]
            assert partition_resource
            partition_resource = partition_resource[0]
        else:
            partition_resources = [x for x in dataset_package['resources'] if x['name'] == partition.name]
            assert len(partition_resources) == 1
            partition_resource = partition_resources[0]

        self.assertEqual(partition_resource['name'], partition.name)

        # FIXME: Read and test the content of the partition.
