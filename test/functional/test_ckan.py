# -*- coding: utf-8 -*-

try:
    # py2, mock is external lib.
    from mock import patch
except ImportError:
    # py3, mock is included
    from unittest.mock import patch

from ambry.orm.database import Database
from ambry.run import get_runconfig
from ambry.exporters.ckan.core import export, MISSING_CREDENTIALS_MSG

from test.factories import FileFactory
from test.test_base import TestBase

# CKAN is mocked by default. If you really want to hit CKAN instance set MOCK_CKAN to False.
MOCK_CKAN = True


class Test(TestBase):

    def setUp(self):
        rc = get_runconfig()
        if 'ckan' not in rc.accounts:
            raise EnvironmentError(MISSING_CREDENTIALS_MSG)
        self.sqlite_db = Database('sqlite://')
        self.sqlite_db.create()
        self._requests = {}  # calls to CKAN mock.
        self._files = {}  # file sent to CKAN mock.

    def test_dataset_export(self):
        bundle = self.setup_bundle('simple', source_url='temp://')
        FileFactory._meta.sqlalchemy_session = bundle.dataset._database.session

        bundle.dataset.config.metadata.about.access = 'public'
        bundle.dataset.config.metadata.contacts.creator.name = 'creator'
        bundle.dataset.config.metadata.contacts.creator.email = 'creator@example.com'
        bundle.dataset.config.metadata.contacts.maintainer.name = 'maintainer'
        bundle.dataset.config.metadata.contacts.maintainer.email = 'maintainer@example.com'

        FileFactory(
            id=20, dataset=bundle.dataset, path='schema.csv',
            contents='table,datatype,size,column,description')
        FileFactory(
            id=21, dataset=bundle.dataset,
            path='documentation.md', contents='### Documentation')

        self.sqlite_db.commit()

        if MOCK_CKAN:
            def fake_call(action, **kwargs):
                if action not in self._requests:
                    self._requests[action] = []
                if action not in self._files:
                    self._files[action] = []
                self._requests[action].append(kwargs['data_dict'])
                if 'files' in kwargs and 'upload' in kwargs['files']:
                    self._files[action].append(kwargs['files']['upload'])

            with patch('ambry.exporters.ckan.core.ckanapi.RemoteCKAN.call_action', side_effect=fake_call):
                export(bundle)
                self._assert_dataset_published(bundle.dataset)
                self._assert_schema_published(bundle.dataset)
        else:
            # Real requests to CKAN instance.
            export(bundle)

            # retrieve saved document from remote CKAN
            from ambry.exporters.ckan.core import ckan
            params = {'q': 'name:{}'.format(bundle.dataset.vid)}
            resp = ckan.action.package_search(**params)
            self.assertEqual(len(resp['results']), 1)
            dataset_package = resp['results'][0]

            self._assert_dataset_published(bundle.dataset, dataset_package=dataset_package)
            self._assert_schema_published(bundle.dataset, dataset_package=dataset_package)

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
            self.assertEqual(len(schema_resource), 1)
            self.assertIn('resource_create', self._files)
        else:
            assert dataset_package
            schema_resources = [x for x in dataset_package['resources'] if x['name'] == 'schema']
            assert len(schema_resources) == 1
            schema_resource = schema_resources[0]
