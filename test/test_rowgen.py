
from test_base import  TestBase
from ambry.bundle.rowgen import DelimitedRowGenerator

class Test(TestBase):
 
    def setUp(self):
        pass

    def  test_basic(self):
        from test import support
        from os.path import join, dirname

        fn = lambda x: join(dirname(support.__file__), x)

        rg = DelimitedRowGenerator(fn('rowgen_basic.csv'))

        for row in rg:
            print row

        print rg.header

    def test_headers(self):
        from test import support
        from os.path import join, dirname
        from ambry.bundle.rowgen import DelimitedRowGenerator

        fn = lambda x: join(dirname(support.__file__), x)

        class RG(DelimitedRowGenerator):
            def is_data_line(self, i, row):
                try:
                    return len(filter(bool,row)) > 5 and int(row[0])
                except ValueError:
                    return False

            def is_header_comment_line(self, i, row):
                return len(filter(bool,row)) < 2

            def is_header_line(self, i, row):
                return len(filter(bool,row)) > 2

        rg = RG(fn('rowgen_multiheader.csv'))

        print rg.intuit_row_spec()

        print rg.get_header()

        for row in rg:
            print row

