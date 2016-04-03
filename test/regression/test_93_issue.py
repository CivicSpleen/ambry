# -*- coding: utf-8 -*-

from ambry.orm import Dataset, Config

from test.proto import TestBase

# https://github.com/CivicKnowledge/ambry/issues/93 bug tests


class Test(TestBase):

    def _assert_deletes(self, replace_part, replacement, assertions=None):
        if not assertions:
            assertions = []

        # populate database with initial bundle.yaml
        bundle = self.import_single_bundle('build.example.com/generators')
        bundle.sync_in()

        # retrieve config from database to be sure all saved properly.
        contacts = bundle.library.dataset(bundle.dataset.vid).config.metadata.contacts
        self.assertEqual(contacts.analyst.url, 'http://example.com')

        # now delete key in the bundle.yaml and sync_in again
        bundle_content = bundle.source_fs.open('bundle.yaml').read()
        try:
            with bundle.source_fs.open('bundle.yaml', 'w') as f:
                assert replace_part in bundle_content
                f.write(bundle_content.replace(replace_part, replacement))
            bundle.sync_in()

            for assert_fn in assertions:
                assert_fn(bundle)
        finally:
            # restore original content of the bundle.yaml
            with bundle.source_fs.open('bundle.yaml', 'w') as f:
                f.write(bundle_content)
            bundle.close()

    def test_deletes_removed_keys_from_db(self):
        replace_part = '' \
            '    analyst:\n' \
            '        org: Example Com\n' \
            '        url: http://example.com\n'
        replacement = ''\
            '    ananlyst:\n' \
            '        org: Example Com\n'

        def _assert_key_deleted(bundle):
            # Assert the key is deleted from db.
            # This one is tricky because we may have two configs with same keys - contacts.analyst.url
            # and contacts.creator.url for example.
            session = bundle.library.database.session
            for config in session.query(Config).filter_by(key='url', value='http://example.com').all():
                assert config.parent.key != 'analyst'

        def _assert_config_key_empty(bundle):
            # Check config built from db - deleted key value should be empty.
            dataset = bundle.library.database.session.query(Dataset).filter_by(vid=bundle.dataset.vid).one()
            contacts = dataset.config.metadata.contacts
            assert contacts.analyst.url == ''

        self._assert_deletes(
            replace_part, replacement,
            assertions=[_assert_key_deleted, _assert_config_key_empty])

    def test_deletes_removed_group_from_db1(self):
        replace_part = '' \
            '    analyst:\n' \
            '        org: Example Com\n' \
            '        url: http://example.com\n'
        replacement = ''

        def _assert_group_deleted(bundle):
            session = bundle.library.database.session
            query = session.query(Config)
            for config in query.filter_by(key='url', value='http://example.com').all():
                assert config.parent.key != 'analyst'

            for config in query.filter_by(key='org', value='Example Com').all():
                assert config.parent.key != 'analyst'

        def _assert_config_group_empty(bundle):
            # Check config built from db - deleted key value should be empty.
            session = bundle.library.database.session
            dataset = session.query(Dataset).filter_by(vid=bundle.dataset.vid).one()
            contacts = dataset.config.metadata.contacts
            assert contacts.analyst == {}
            assert contacts.analyst.url == ''
            assert contacts.analyst.org == ''

        self._assert_deletes(
            replace_part, replacement,
            assertions=[_assert_group_deleted, _assert_config_group_empty])
