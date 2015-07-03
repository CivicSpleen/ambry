# -*- coding: utf-8 -*-
import unittest

from ambry.metadata.schema import Top

from ambry.orm import Config

from test.test_base import TestBase

from ambry.metadata.schema import About, Top


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
            # versions',  # FIXME: uncomment and implement.
            'names',
            'documentation',
            'coverage',
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
        top.about.footnote = 'the-footnone'
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
        top.about.time = '15:55'  # TODO: How to convert time?
        top.about.title = 'Inpatient Mortality Indicators'

        # build from db and check
        new_top = Top()
        new_top.build_from_db(dataset)
        self.assertEquals(new_top.about.access, 'restricted')
        self.assertEquals(top.about.footnote, 'the-footnone')
        self.assertEquals(top.about.grain, 'hospital')
        self.assertEquals(top.about.groups, ['health', 'california'])
        self.assertEquals(top.about.license, 'ckdbl')
        self.assertEquals(top.about.processed, 'processed')
        self.assertEquals(top.about.rights, 'public')
        self.assertEquals(top.about.source, 'http://example.com')
        self.assertEquals(top.about.space, 'California')
        self.assertEquals(top.about.subject, 'Subject')
        self.assertEquals(top.about.summary, 'The Inpatient Mortality Indicators (IMIs) are a subset of...')
        self.assertEquals(top.about.tags, ['tag1', 'tag2'])
        self.assertEquals(top.about.time, '15:55')  # TODO: How to convert time?
        self.assertEquals(top.about.title, 'Inpatient Mortality Indicators')
