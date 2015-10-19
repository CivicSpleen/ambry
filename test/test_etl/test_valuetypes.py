# -*- coding: utf-8 -*-

from test.test_base import TestBase



class Test(TestBase):


    def test_basic(self):

        from ambry.valuetype.usps import StateAbr

        sa =  StateAbr('AZ')
        self.assertEqual('AZ' ,sa)
        self.assertEqual(4, sa.fips)
        self.assertEqual('Arizona', sa.name)

        # Convert to a FIPS code
        self.assertEqual('04', sa.fips.str)
        self.assertEqual('04000US04', sa.fips.geoid)
        self.assertEqual('04', sa.fips.tiger)
        self.assertEqual('0E04', sa.fips.gvid)
        self.assertEqual('Arizona', sa.fips.name)
        self.assertEqual('AZ', sa.fips.usps.fips.usps)

        from ambry.valuetype.census import AcsGeoid

        g = AcsGeoid('15000US530330018003')

        self.assertEqual('Washington', g.state.name)
        self.assertEqual('WA', g.state.usps)

    def test_clean_transform(self):
        from ambry.dbexceptions import ConfigurationError
        from ambry.orm.column import Column

        ct = Column.clean_transform

        self.assertEqual('^init|t1|t2|t3|t4|!except' ,
                         ct('!except|t1|t2|t3|t4|^init'))

        self.assertEqual( '^init|t1|t2|t3|t4|!except||t1|t2|t3|t4|!except',
                          ct('t1|^init|t2|!except|t3|t4||t1|t2|!except|t3|t4'))

        self.assertEqual('^init|t1|t2|t3|t4|!except||t4',
                         ct('t1|^init|t2|!except|t3|t4||t4'))

        self.assertEqual('^init|t1|t2|t3|t4|!except',
                         ct('t1|^init|t2|!except|t3|t4||||'))

        self.assertEqual( '^init|t1|t2|t3|t4|!except',
                          ct('|t1|^init|t2|!except|t3|t4||||'))

        self.assertEqual('^init', ct('^init'))

        self.assertEqual('!except',ct('!except'))

        self.assertEqual(ct('||transform2'), '||transform2')

        with self.assertRaises(ConfigurationError): # Init in second  segment
            ct('t1|^init|t2|!except|t3|t4||t1|^init|t2|!except|t3|t4')

        with self.assertRaises(ConfigurationError): # Two excepts in a segment
            print ct('t1|^init|t2|!except|t3|t4||!except1|!except2')

        with self.assertRaises(ConfigurationError):  # Two inits in a segment
            print ct('t1|^init|t2|^init|!except|t3|t4')

    def test_col_clean_transform(self):

        b = self.setup_bundle('casters')
        b.sync_in();  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        for t in b.tables:
            for c in t.columns:
                print c.name, c.transform, c.expanded_transform

    def test_table_transforms(self):

        b = self.setup_bundle('casters')
        b.sync_in();  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        t = b.table('simple_stats')

        self.assertEqual(3, len(list(t.transforms)))

        for tr in t.transforms:
            print tr


    def test_code_calling_pipe(self):
        import re

        from ambry.etl import CastColumns, RowProxy

        b = self.setup_bundle('casters')
        b.sync_in();  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        pl = b.pipeline(source=b.source('simple_stats'))

        ccp = pl[CastColumns]

        source_table = ccp.source.source_table
        dest_table = ccp.source.dest_table

        headers = [ c.source_header for c in ccp.source.source_table.columns]

        r =  re.sub(r'\s','',ccp.compose_column(1, 1, 'header', dest_table.column('float_a'),
                                  dict(init='cst_init', transforms=['a', 'b'], exception=None)))

        self.assertEqual("""f_1_2_float_a(f_1_1_float_a(cst_init(row[1]),1,2,'header','float_a',row,row_n,scratch,errors,pipe,bundle,source),1,2,'header','float_a',row,row_n,scratch,errors,pipe,bundle,source)""",
                         r)

        r = re.sub(r'\s','',ccp.compose_column( 1, 1, 'header', dest_table.column('float_a'),
                                  dict(init='cst_init',transforms=['a','b'], exception = 'cst_exception')))
        self.assertEqual("""tc_exc_1_float_a(row[1],1,2,'header','float_a',row=row,row_n=row_n,scratch=scratch,errors=errors,pipe=pipe,bundle=bundle,source=source)""",
                         r)


        return

        #print ccp.compose_column(1, 'SH', ccp.source.dest_table.column('int_a') )
        #print ccp.compose(headers)

        row = [1.0,1.0,1.0,1,1,"one","two"]

        ccp.process_header(headers)
        print ccp.process_body(row)
        print ccp

        row = [1.0, 1.0, 1.0, 1, 'exception', "one", "two"]

        ccp.process_header(headers)
        print ccp.process_body(row)
        #print ccp

