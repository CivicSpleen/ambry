# -*- coding: utf-8 -*-
from ambry.metadata.schema import Top

from test.test_base import TestBase


class TopTest(TestBase):

    # helpers
    def _assert_fields_match(self, expected_fields, dest):
        """ Asserts that `dest` object has all fields listed in the `expected_fields`.

        expected_fields (list): list of string with names to find in the `dest` fields.
        dest (object): testing object.

        """

        assert expected_fields
        for field in expected_fields:
            self.assertTrue(hasattr(dest, field), '{} is missing {} field.'.format(dest, field))

    def test_top_level_fields(self):
        """ Test top level groups of the structured property tree. """
        expected_fields = [
            'about',
            'identity',
            'dependencies',
            'external_documentation',
            'build',
            'contacts',
            'versions',
            'names',
        ]

        self._assert_fields_match(expected_fields, Top())

    def test_about_group_fields(self):
        """ Test about group of the metadata config. """
        expected_fields = [
            'access',
            'footnote',
            'grain',
            'groups',
            'license',
            'processed',
            'rights',
            'source',
            'space',
            'subject',
            'summary',
            'tags',
            'time',
            'title'
        ]

        self._assert_fields_match(expected_fields, Top().about)

    def test_about_group_fields_values(self):
        """ Test about group fields values. """
        # Test both - setting and saving to db.
        top = Top()
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        top.link_config(db.session, dataset)

        top.about.access = 'restricted'
        top.about.footnote = 'the-footnote'
        top.about.grain = 'hospital'
        top.about.groups = ['health', 'california']
        top.about.license = 'ckdbl'
        top.about.processed = 'processed'
        top.about.rights = 'public'
        top.about.source = 'http://example.com'
        top.about.space = 'California'
        top.about.subject = 'Subject'
        top.about.summary = 'The Inpatient Mortality Indicators (IMIs) are a subset of...'
        top.about.tags = ['tag1', 'tag2']
        top.about.time = '15:55'  # TODO: How to convert time?  ESB: You don't; it's usually an ISO duration, or integer year.
        top.about.title = 'Inpatient Mortality Indicators'

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(new_top.about.access, 'restricted')
        self.assertEquals(new_top.about.footnote, 'the-footnote')
        self.assertEquals(new_top.about.grain, 'hospital')
        self.assertEquals(new_top.about.groups, ['health', 'california'])
        self.assertEquals(new_top.about.license, 'ckdbl')
        self.assertEquals(new_top.about.processed, 'processed')
        self.assertEquals(new_top.about.rights, 'public')
        self.assertEquals(new_top.about.source, 'http://example.com')
        self.assertEquals(new_top.about.space, 'California')
        self.assertEquals(new_top.about.subject, 'Subject')
        self.assertEquals(
            new_top.about.summary,
            'The Inpatient Mortality Indicators (IMIs) are a subset of...')

        self.assertEquals(new_top.about.tags, ['tag1', 'tag2'])
        self.assertEquals(new_top.about.time, '15:55')  # TODO: How to convert time?
        self.assertEquals(new_top.about.title, 'Inpatient Mortality Indicators')

    def test_identity_fields(self):
        """ Test identity group of the metadata config. """
        expected_fields = [
            'bspace',
            'btime',
            'dataset',
            'id',
            'revision',
            'source',
            'subset',
            'type',
            'variation',
            'version',
        ]

        self._assert_fields_match(expected_fields, Top().identity)

    def test_identity_fields_values(self):
        """ Test contacts group fields of the metadata config. """
        # Test both - setting and saving to db.
        top = Top()
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        top.link_config(db.session, dataset)

        top.identity.bspace = 'b-space'
        top.identity.btime = 'b-time'
        top.identity.dataset = dataset.vid
        top.identity.id = dataset.id
        top.identity.revision = 7
        top.identity.source = 'example.com'
        top.identity.subset = 'mortality'
        top.identity.type = '?'
        top.identity.variation = 1
        top.identity.version = '0.0.7'

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(new_top.identity.bspace, 'b-space')
        self.assertEquals(new_top.identity.btime, 'b-time')
        self.assertEquals(new_top.identity.dataset, dataset.vid)
        self.assertEquals(new_top.identity.id, dataset.id)
        self.assertEquals(new_top.identity.revision, 7)
        self.assertEquals(new_top.identity.source, 'example.com')
        self.assertEquals(new_top.identity.subset, 'mortality')
        self.assertEquals(new_top.identity.type, '?')
        self.assertEquals(new_top.identity.variation, 1)
        self.assertEquals(new_top.identity.version, '0.0.7')

    def test_dependencies_fields(self):
        """ Test dependencies group of the metadata config. """
        expected_fields = [
            'counties',
            'facility_index',
            'facility_info',
        ]

        self._assert_fields_match(expected_fields, Top().dependencies)

    def test_dependencies_fields_values(self):
        """ Test Top.dependencies group fields of the metadata config. """
        # Test both - setting and saving to db.
        top = Top()
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        top.link_config(db.session, dataset)

        top.dependencies.counties = 'census.gov-index-counties'
        top.dependencies.facility_index = 'oshpd.ca.gov-facilities-index-facilities_index-2010e2014'
        top.dependencies.facility_info = 'oshpd.ca.gov-facilities-index-facilities'

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(
            new_top.dependencies.counties,
            'census.gov-index-counties')
        self.assertEquals(
            new_top.dependencies.facility_index,
            'oshpd.ca.gov-facilities-index-facilities_index-2010e2014')
        self.assertEquals(
            new_top.dependencies.facility_info,
            'oshpd.ca.gov-facilities-index-facilities')

    def test_requirements_fields(self):
        """ Test requirements group of the metadata config. """
        # requirements group allow any field
        expected_fields = [
            'xlrd',
            'requests',
            'suds',
        ]
        top = Top()
        top.requirements.xlrd = 'xlrd'
        top.requirements.requests = 'requests'
        top.requirements.suds = 'suds'

        self._assert_fields_match(expected_fields, top.requirements)

    def test_requirements_fields_values(self):
        """ Test Top.requirements group fields of the metadata config. """
        # Test both - setting and saving to db.
        top = Top()
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        top.link_config(db.session, dataset)

        top.requirements.xlrd = 'xlrd'
        top.requirements.requests = 'requests'
        top.requirements.suds = 'suds'

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(
            new_top.requirements.xlrd,
            'xlrd')
        self.assertEquals(
            new_top.requirements.requests,
            'requests')
        self.assertEquals(
            new_top.requirements.suds,
            'suds')

    # Top().external_documentation tests
    def test_external_documentation_fields(self):
        """ Test external_documentation group of the metadata config. """
        # requirements group allow any field
        expected_fields = [
            'url',
            'title',
            'description',
            'source'
        ]
        self._assert_fields_match(expected_fields, Top().external_documentation.any_random_field)

    def test_external_documentation_fields_values(self):
        """ Test Top.external_documentation group fields of the metadata config. """
        # Test both - setting and saving to db.
        top = Top()
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        top.link_config(db.session, dataset)

        top.external_documentation.any_field.url = 'http://example.com'
        top.external_documentation.any_field.title = 'the-title'
        top.external_documentation.any_field.description = 'the-description'
        top.external_documentation.any_field.source = 'http://example.com'

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(
            new_top.external_documentation.any_field.url,
            'http://example.com')
        self.assertEquals(
            new_top.external_documentation.any_field.title,
            'the-title')
        self.assertEquals(
            new_top.external_documentation.any_field.description,
            'the-description')
        self.assertEquals(
            new_top.external_documentation.any_field.source,
            'http://example.com')

    def test_build_fields(self):
        """ Test build group of the metadata config. """
        # build group allows any field
        expected_fields = [
            'key1',
            'key2',
            'key3',
        ]
        top = Top()
        top.build.key1 = 'value1'
        top.build.key2 = 'value2'
        top.build.key3 = 'value3'

        self._assert_fields_match(expected_fields, top.build)

    def test_build_fields_values(self):
        """ Test Top().build group fields of the metadata config. """
        # Test both - setting and saving to db.
        top = Top()
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        top.link_config(db.session, dataset)

        top.build.key1 = 'value1'
        top.build.key2 = 'value2'
        top.build.key3 = 'value3'

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(
            new_top.build.key1,
            'value1')
        self.assertEquals(
            new_top.build.key2,
            'value2')
        self.assertEquals(
            new_top.build.key3,
            'value3')

    # Top().names tests
    def test_names_fields(self):
        """ Test Top().names group of the metadata config. """
        expected_fields = [
            'fqname',
            'name',
            'vid',
            'vname',
        ]

        self._assert_fields_match(expected_fields, Top().names)

    def test_names_fields_values(self):
        """ Test Top().names group fields of the metadata config. """
        # Test both - setting and saving to db.
        top = Top()
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        top.link_config(db.session, dataset)

        top.names.fqname = 'fq-name'
        top.names.name = 'name'
        top.names.vid = 'd001'
        top.names.vname = 'vname'

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(new_top.names.fqname, 'fq-name')
        self.assertEquals(new_top.names.name, 'name')
        self.assertEquals(new_top.names.vid, 'd001')
        self.assertEquals(new_top.names.vname, 'vname')

    # Top().contacts tests
    def test_contacts_fields(self):
        """ Test contacts group of the metadata config. """
        expected_fields = [
            'creator',
            'maintainer',
            'source',
            'analyst',
        ]

        self._assert_fields_match(expected_fields, Top().contacts)

    def test_contacts_fields_values(self):
        """ Test contacts group fields of the metadata config. """
        # Test both - setting and saving to db.
        top = Top()
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        top.link_config(db.session, dataset)

        top.contacts.creator.role = 'c-developer'
        top.contacts.creator.org = 'c-home'
        top.contacts.creator.email = 'c.test@example.com'
        top.contacts.creator.name = 'c-tester'
        top.contacts.creator.url = 'http://creator.example.com'
        # FIXME: Populate maintainer, source and analyst too.

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(new_top.contacts.creator.role, 'c-developer')
        self.assertEquals(new_top.contacts.creator.org, 'c-home')
        self.assertEquals(new_top.contacts.creator.email, 'c.test@example.com')
        self.assertEquals(new_top.contacts.creator.name, 'c-tester')
        self.assertEquals(new_top.contacts.creator.url, 'http://creator.example.com')

    # Top().versions tests
    def test_versions_fields(self):
        """ Test contacts group of the metadata config. """
        expected_fields = [
            'version',
            'date',
            'description',
        ]

        self._assert_fields_match(expected_fields, Top().versions.any_field)

    def test_versions_fields_values(self):
        """ Test Top().versions group fields of the metadata config. """
        # Test both - setting and saving to db.
        top = Top()
        db = self.new_database()
        dataset = self.new_db_dataset(db, n=0)
        top.link_config(db.session, dataset)

        top.versions['1'].date = '2015-04-12T15:49:55.077036'
        top.versions['1'].description = 'Adding coverage'
        top.versions['1'].version = '0.0.2'

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(new_top.versions['1'].date, '2015-04-12T15:49:55.077036')
        self.assertEquals(new_top.versions['1'].description, 'Adding coverage')
        self.assertEquals(new_top.versions['1'].version, '0.0.2')
