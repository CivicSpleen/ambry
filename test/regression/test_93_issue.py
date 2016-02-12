# -*- coding: utf-8 -*-

from ambry.orm import Dataset, Config

from test.proto import TestBase

# https://github.com/CivicKnowledge/ambry/issues/93 bug tests


class Test(TestBase):

    def test_deletes_removed_keys_from_db(self):
        # library = self.library()

        # populate database with initial bundle.yaml
        bundle = self.import_single_bundle('build.example.com/simple')
        bundle.sync_in()

        # retrieve config from database to be sure all saved properly.
        contacts = bundle.library.dataset(bundle.dataset.vid).config.metadata.contacts
        self.assertEqual(contacts.analyst.url, 'http://example.com')

        # now delete key in the bundle.yaml and sync_in again
        bundle_content = bundle.source_fs.open('bundle.yaml').read()
        with bundle.source_fs.open('bundle.yaml', 'w') as f:
            replace_part = '' \
                '    analyst:\n' \
                '        org: Example Com\n' \
                '        url: http://example.com\n'
            replacement = ''\
                '    ananlyst:\n' \
                '        org: Example Com\n'
            assert replace_part in bundle_content
            f.write(bundle_content.replace(replace_part, replacement))
        bundle.sync_in()

        # Assert the key is deleted from db.
        # This one is tricky because we may have two configs with same keys - contacts.analyst.url
        # and contacts.creator.url for example.
        session = bundle.library.database.session
        for config in session.query(Config).filter_by(key='url', value='http://example.com').all():
            self.assertNotEqual(config.parent.key, 'analyst')

        # Check config built from db - deleted key value should be empty.
        dataset = session.query(Dataset).filter_by(vid=bundle.dataset.vid).one()
        contacts = dataset.config.metadata.contacts
        self.assertEqual(contacts.analyst.url, '')

    def test_deletes_removed_group_from_db(self):

        # populate database with initial bundle.yaml
        bundle = self.import_single_bundle('build.example.com/simple')
        bundle.sync_in()

        # retrieve config from database to be sure all saved properly.
        contacts = bundle.library.dataset(bundle.dataset.vid).config.metadata.contacts
        self.assertEqual(contacts.analyst.url, 'http://example.com')

        # now delete analyst group in the bundle.yaml and sync_in again
        bundle_content = bundle.source_fs.open('bundle.yaml').read()
        with bundle.source_fs.open('bundle.yaml', 'w') as f:
            replace_part = '' \
                '    analyst:\n' \
                '        org: Example Com\n' \
                '        url: http://example.com\n'
            assert replace_part in bundle_content

            new_content = bundle_content.replace(replace_part, '')
            f.write(new_content)
        bundle.sync_in()

        # Check the key is deleted from db.
        session = bundle.library.database.session
        query = session.query(Config)
        self.assertIsNone(query.filter_by(key='analyst').first())

        # Analyst inner terms removed too.
        # This one is tricky because we may have two configs with the same insides - contacts.analyst
        # and contacts.creator for example.
        for config in query.filter_by(key='url', value='http://example.com').all():
            self.assertNotEqual(config.parent.key, 'analyst')

        for config in query.filter_by(key='org', value='Example Com').all():
            self.assertNotEqual(config.parent.key, 'analyst')

        # Check config built from db - deleted key value should be empty.
        dataset = session.query(Dataset).filter_by(vid=bundle.dataset.vid).one()
        contacts = dataset.config.metadata.contacts
        self.assertEqual(contacts.analyst, {})
        self.assertEqual(contacts.analyst.url, '')
        self.assertEqual(contacts.analyst.org, '')
