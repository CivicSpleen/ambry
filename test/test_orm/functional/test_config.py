# -*- coding: utf-8 -*-
import unittest

from sqlalchemy.exc import IntegrityError

from ambry.orm.config import Config

from test.test_base import TestBase


class Test(TestBase):

    def test_id_generation(self):

        # set some configs
        db = self.new_database()
        ds = self.new_db_dataset(db, n=0)

        ds.config.metadata.identity.id = 'd02'
        ds.config.metadata.identity.version = '0.0.1'
        db.commit()

        # check ids
        query = db.session.query(Config)
        for config in query.filter_by(d_vid=ds.vid).all():
            self.assertTrue(config.id.startswith('F'))
            self.assertIn(ds.id[1:], config.id)

    def test_dataset_config_operations(self):
        """Basic operations on datasets"""

        db = self.new_database()
        ds = self.new_db_dataset(db, n=0)

        ds.config.metadata.identity.id = 'd02'
        ds.config.metadata.identity.version = '0.0.1'
        db.commit()

        # need to refresh dataset after commit
        ds = db.dataset(self.dn[0])

        self.assertEqual(ds.config.metadata.identity.id, 'd02')
        self.assertEqual(ds.config.metadata.identity.version, '0.0.1')

        self.assertEqual(10, len(ds.config.metadata.identity))

        identity_keys = [
            'subset', 'variation', 'dataset', 'btime', 'source',
            'version', 'bspace', 'type', 'id', 'revision']

        self.assertEqual(sorted(identity_keys), sorted([v for v in ds.config.metadata.identity]))

        with self.assertRaises(AttributeError):
            ds.config.metadata = 'foo'

        try:
            ds.config.metadata.identity = [1, 2, 3]
        except AssertionError as exc:
            self.assertIn('Dictionary is required', exc.message)

    def test_unique(self):
        """ d_vid, type, group and key are unique together. """
        db = self.new_database()
        ds = self.new_db_dataset(db, n=0)
        conf1 = Config(d_vid=ds.vid, type='metadata', group='identity', key='key1', value='value1')
        db.session.add(conf1)
        db.session.commit()

        dupe = Config(d_vid=ds.vid, type='metadata', group='identity', key='key1', value='value1')
        db.session.add(dupe)
        try:
            db.session.commit()
            raise AssertionError('Dupe unexpectedly saved. It seems unique constraint is broken.')
        except IntegrityError as exc:
            self.assertIn('UNIQUE constraint failed', str(exc))

    @unittest.skip("Credentials need to be fixed")
    def test_config_postgres_unicode(self):

        from ambry.orm.database import Database
        import time

        rc = self.get_rc()

        dsn = rc.database('pg-func-test', return_dsn=True)

        print(dsn)

        db = Database(dsn)

        db.create()

        db.clean()

        ds = db.new_dataset(**self.ds_params(1, source='source'))

        db.commit()

        ds.config.library.build.url = 'http:/foo/bar/baz/øé'

        ds.config.sync['lib']['foobar'] = time.time()

        ds.commit()

        print(ds.config.library.build.url)
