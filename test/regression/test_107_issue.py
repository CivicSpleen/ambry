# -*- coding: utf-8 -*-
import os

from ambry.util import parse_url_to_dict


from test.proto import TestBase


class Test(TestBase):

    def test_search_sqlite_fails(self):
        db_path = parse_url_to_dict(self.config.library.database)['path']

        if os.path.exists(db_path):
            os.remove(db_path)

        library = self.library()

        # Prior to the fix, this triggers the error
        for r in library.search.search('foobar'):
            pass
