
from test.test_base import TestBase
from ambry.library import new_library

class Test(TestBase):

    def test_search_sqlite_fails(self):
        from ambry.util import parse_url_to_dict
        import os

        rc = self.get_rc()

        db_path =  parse_url_to_dict(rc.library()['database'])['path']
        if os.path.exists(db_path):
            os.remove(db_path)

        l = new_library(rc)

        s = l.search

        # Prior to the fix, this triggers the error
        for r in s.search('foobar'):
            print r

