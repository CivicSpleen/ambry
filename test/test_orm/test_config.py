
import unittest
import tempfile
import uuid


from ambry.identity import DatasetNumber


class Test(unittest.TestCase):

    def setUp(self):

        super(Test,self).setUp()

        self.uuid = str(uuid.uuid4())
        self.tmpdir = tempfile.mkdtemp(self.uuid)

        self.delete_tmpdir = True

        self.dsn = "sqlite:///{}/test.db".format(self.tmpdir)

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

    def new_dataset(self, n):
        from ambry.orm import Dataset
        return Dataset(vid=self.dn[n], source='source', dataset='dataset' )

    def dump_config(self, db):
        import sys
        from subprocess import check_output

        print check_output('sqlite3 {} "SELECT * FROM config" '.format(db.path), stderr=sys.stderr, shell=True)

    def test_basic(self):
        """Basic operations on datasets"""

        from ambry.orm.database import Database

        db = Database(self.dsn)
        db.open()

        ds = db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')
        ds.config.meta.identity.foo = [1,2,3,4]
        ds.config.meta.identity.bar = [5,6,7,8]
        ds.config.meta.identity.bar = ['a','b','c','d']
        db.commit()

        db = Database(self.dsn)
        db.open()
        ds = db.dataset(self.dn[0])
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

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite