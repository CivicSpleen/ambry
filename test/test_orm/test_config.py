# -*- coding: utf-8 -*-

from sqlalchemy.exc import IntegrityError

from ambry.orm.config import Config
from test.test_base import TestBase


class Test(TestBase):

    def test_basic(self):
        """Basic operations on datasets"""

        db = self.new_database()
        ds = self.new_db_dataset(db, n=0)

        ds.config.metadata.identity.id = 'd02'
        ds.config.metadata.identity.version = '0.0.1'
        db.commit()

        # need to refresh dataset after commit
        ds = db.dataset(self.dn[0])

        self.assertEquals(ds.config.metadata.identity.id, 'd02')
        self.assertEquals(ds.config.metadata.identity.version, '0.0.1')

        self.assertEquals(10, len(ds.config.metadata.identity))

        identity_keys = [
            'subset', 'variation', 'dataset', 'btime', 'source',
            'version', 'bspace', 'type', 'id', 'revision']

        self.assertItemsEqual(identity_keys, [v for v in ds.config.metadata.identity])

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
            raise AssertionError('Dupe unexpectadly saved. It seems unique constraint is broken.')
        except IntegrityError as exc:
            self.assertIn('UNIQUE constraint failed', exc.message)


def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
