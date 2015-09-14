# -*- coding: utf-8 -*-

from ambry.library import new_library
from ambry.orm import Dataset, Config

from test.test_base import TestBase

# https://github.com/CivicKnowledge/ambry/issues/93 bug tests


class Test(TestBase):

    def test_deletes_removed_keys_from_db(self):
        rc = self.get_rc()
        library = new_library(rc)

        # populate database with initial bundle.yaml
        bundle = self.setup_bundle('simple', library=library)
        bundle.sync_in()

        # retrieve config from database to be sure all saved properly.
        contacts = bundle.library.dataset(bundle.dataset.vid).config.metadata.contacts
        self.assertEqual(contacts.analyst.url, 'http://example.com')

        # now delete key in the bundle.yaml and sync_in again
        bundle_content = bundle.source_fs.open('bundle.yaml').read()
        with bundle.source_fs.open('bundle.yaml', 'w') as f:
            f.write(bundle_content.replace('url: http://example.com', ''))
        bundle.sync_in()

        # Check the key is deleted from db.
        session = bundle.library.database.session
        url_config = session\
            .query(Config)\
            .filter_by(key='url', value='http://example.com')\
            .first()
        self.assertIsNone(url_config)

        # Check config built from db - deleted key value should be empty.
        dataset = session.query(Dataset).filter_by(vid=bundle.dataset.vid).one()
        contacts = dataset.config.metadata.contacts
        self.assertEqual(contacts.analyst.url, '')

    def test_deletes_removed_group_from_db(self):
        rc = self.get_rc()
        library = new_library(rc)

        # populate database with initial bundle.yaml
        bundle = self.setup_bundle('simple', library=library)
        bundle.sync_in()

        # retrieve config from database to be sure all saved properly.
        contacts = bundle.library.dataset(bundle.dataset.vid).config.metadata.contacts
        self.assertEqual(contacts.analyst.url, 'http://example.com')

        # now delete analyst group in the bundle.yaml and sync_in again
        bundle_content = bundle.source_fs.open('bundle.yaml').read()
        with bundle.source_fs.open('bundle.yaml', 'w') as f:
            new_content = bundle_content\
                .replace('url: http://example.com', '')\
                .replace('org: Example Com', '')\
                .replace('analyst:', '')
            f.write(new_content)
        bundle.sync_in()

        # Check the key is deleted from db.
        session = bundle.library.database.session
        query = session.query(Config)
        self.assertIsNone(query.filter_by(key='analyst').first())

        # Analyst inner terms removed too.
        self.assertIsNone(query.filter_by(key='url', value='http://example.org').first())
        self.assertIsNone(query.filter_by(key='org', value='Example Com').first())

        # Check config built from db - deleted key value should be empty.
        dataset = session.query(Dataset).filter_by(vid=bundle.dataset.vid).one()
        contacts = dataset.config.metadata.contacts
        self.assertEqual(contacts.analyst, {})
        self.assertEqual(contacts.analyst.url, '')
        self.assertEqual(contacts.analyst.org, '')
