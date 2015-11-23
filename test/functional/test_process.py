
import os

import yaml

from ambry.run import get_runconfig

from test import bundlefiles
from test.test_base import TestBase


class Test(TestBase):


    def test_basic_process_logging(self):
        from ambry.bundle.process import ProcessLogger
        l = self.library()
        l.drop()
        l.create()

        db = l.database
        print '!!!!', db.dsn


        pl = ProcessLogger(db.root_dataset)

        ps = pl.start(message = 'Starting')
        ps.add('Add')
        ps.add('Add')
        ps.update('Update')
        ps.update('Update')
        del ps


        for r in db.root_dataset.process_records:
            print r
