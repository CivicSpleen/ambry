
import unittest
import tempfile
import uuid
from ambry.orm import Column
from ambry.orm import Partition
from ambry.orm import Table
from ambry.orm import Dataset
from ambry.orm import Config
from ambry.orm import File
from ambry.orm import Code
from ambry.orm import ColumnStat
from sqlalchemy.orm import sessionmaker
from ambry.identity import DatasetNumber, PartitionNumber
from sqlalchemy.exc import IntegrityError

class Test(unittest.TestCase):

    def setUp(self):
        from sqlalchemy import create_engine

        super(Test,self).setUp()

        self.dsn = 'sqlite://'

        self.ds_params = dict(
            vid=str(DatasetNumber(1, 1)), source='source', dataset='dataset'
        )


    def dump_database(self, db, table):
        import sys
        from subprocess import check_output

        for row in db.connection.execute("SELECT * FROM {}".format(table)):
            print row


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

