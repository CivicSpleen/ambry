
from test.test_base import TestBase
import tempfile
import uuid


from ambry.identity import DatasetNumber


class Test(TestBase):


    def test_basic(self):
        """Basic operations on datasets"""

        from ambry.orm.database import Database

        db = self.new_database()
        ds = self.new_db_dataset(db, n=0)

        ds.config.meta.identity.foo = [1,2,3,4]
        ds.config.meta.identity.bar = [5,6,7,8]
        ds.config.meta.identity.bar = ['a','b','c','d']
        db.commit()

        ds = db.dataset(self.dn[0]) # need to refresh dataset after commit

        self.assertItemsEqual([1, 2, 3, 4], list(ds.config.meta.identity.foo))
        self.assertItemsEqual(['a','b','c','d'], list(ds.config.meta.identity.bar))

        self.assertEquals(2,len(ds.config.meta.identity))

        self.assertItemsEqual(['foo','bar'], [v for v in ds.config.meta.identity] )

        self.assertItemsEqual([1, 2, 3, 4], dict(ds.config.meta.identity.items())['foo'])
        self.assertItemsEqual(['a', 'b', 'c', 'd'], dict(ds.config.meta.identity.items())['bar'])

        self.assertEquals('<config: d000000001001,identity,foo = [1, 2, 3, 4]>',
                          str(dict(ds.config.meta.identity.records())['foo']))

        self.assertEquals('metadata', dict(ds.config.meta.identity.records())['foo'].dict['type'])

        with self.assertRaises(AttributeError):
            ds.config.meta = 'foo'

        with self.assertRaises(AttributeError):
            ds.config.meta.identity = [1,2,3]

        with self.assertRaises(AttributeError):
            ds.config.meta.identity._dataset = None


    def test_assignment(self):
        pass

def suite():
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite