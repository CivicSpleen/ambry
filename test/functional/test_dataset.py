# -*- coding: utf-8 -*-

from ambry.identity import DatasetNumber
from ambry.orm.exc import ConflictError

from test.proto import TestBase


class Test(TestBase):

    def setUp(self):

        super(Test, self).setUp()

        # Make an array of dataset numbers, so we can refer to them with a single integer
        self.dn = [str(DatasetNumber(x, x)) for x in range(1, 10)]

    def test_dataset_basic(self):
        """Basic operations on datasets"""
        library = self.library(use_proto=False)
        db = library.database

        try:
            # Creating and conflicts
            #
            db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')
            db.new_dataset(vid=self.dn[1], source='source', dataset='dataset')

            with self.assertRaises(ConflictError):
                db.new_dataset(vid=self.dn[0], source='source', dataset='dataset')

            dn = DatasetNumber(100)

            # datasets() gets datasets, and latest give id instead of vid
            #
            db.new_dataset(vid=str(dn.rev(5)), source='a', dataset='dataset')
            db.new_dataset(vid=str(dn.rev(1)), source='a', dataset='dataset')
            db.new_dataset(vid=str(dn.rev(3)), source='a', dataset='dataset')
            db.new_dataset(vid=str(dn.rev(4)), source='a', dataset='dataset')

            ds = db.dataset(str(dn.rev(5)))
            self.assertEqual(str(dn.rev(5)), ds.vid)

            ds = db.dataset(str(dn.rev(3)))
            self.assertEqual(str(dn.rev(3)), ds.vid)

            ds = db.dataset(str(dn.rev(None)))
            self.assertEqual(str(dn.rev(5)), ds.vid)

            db.new_dataset(vid=str(dn.rev(6)), source='a', dataset='dataset')

            ds = db.dataset(str(dn.rev(None)))
            self.assertEqual(str(dn.rev(6)), ds.vid)
        finally:
            db.close()

    def test_config(self):

        db = self.library().database

        try:
            db.root_dataset.config.library.config.path = 'foobar'
            self.assertEqual('foobar',  db.root_dataset.config.library.config.path)
        finally:
            db.close()
