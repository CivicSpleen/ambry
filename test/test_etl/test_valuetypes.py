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

        from ambry.orm.column import Column

        c = Column(name='column', sequence_id = 1, datatype = 'int')

        c.transform = 't1|^init|t2|!except|t3|t4'
        self.assertEqual(['init', None], [e['init'] for e in c.expanded_transform])
        self.assertEqual([[int], ['t1', 't2', 't3', 't4']], [e['transforms'] for e in c.expanded_transform])


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
        from collections import namedtuple
        from ambry.valuetype.exceptions import CastingError
        from ambry.valuetype.fips import State

        b = self.setup_bundle('casters')
        b.sync_in();  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        pl = b.pipeline(source=b.source('simple_stats'))

        ccp = pl[CastColumns]

        source_table = ccp.source.source_table
        dest_table = ccp.source.dest_table

        source_headers = [ c.source_header for c in source_table.columns]

        # ---

        def run_col(v, xform, type_=int):
            import ambry.orm.column

            row = [v]

            Column = namedtuple('Column','name, sequence_id, valuetype_class')

            for segment in ambry.orm.column.Column._expand_transform(xform):

                code =  ccp.compose_column('pl0',0,'header',
                                         Column._make(['column','0',type_]),
                                         segment)

                #print ccp.pretty_code
                #print code

                func = eval('lambda row,row_n=11,scratch=[],errors=[],pipe=None, source=None, bundle=None:'
                                +code, ccp.env())

                row = [func(row)]

            return row[0]

        self.assertEqual(8, run_col(2,'cst_double|cst_double'))
        self.assertEqual(16, run_col(2,'cst_double|cst_double|cst_double'))
        self.assertEqual(8, run_col(2,'^cst_init|cst_double|cst_double|cst_double'))

        with self.assertRaises(CastingError):
            self.assertEqual(8, run_col(2,'^"foo"|cst_double'))

        with self.assertRaises(ValueError):
            self.assertEqual(8, run_col(2,'^"foo"|cst_double|!cst_reraise_value'))

        self.assertEqual(32, run_col(2, 'cst_double|cst_double||cst_double|cst_double'))

        self.assertEqual('California', run_col(6, 'v.name', State ))

        ccp.process_header(source_headers)

        self.assertEquals([1, 2.0, 4.0, 16.0, 1, 1, None, 'ONE', 'TXo', 1, 'Alabama'],
                          ccp.process_body([1.0,1.0,1.0,1,1,"one","two"]))

        self.assertEqual([2, 2.0, 4.0, 16.0, 1, None, 'exception', 'ONE', 'TXo', 1, 'Alabama'],
                         ccp.process_body([1.0, 1.0, 1.0, 1, 'exception', "one", "two"]))

