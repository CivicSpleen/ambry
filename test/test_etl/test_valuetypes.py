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

    def test_code_calling_pipe(self):

        from ambry.etl import CastColumns, RowProxy

        b = self.setup_bundle('casters')
        b.sync_in();  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        pl = b.pipeline(source=b.source('simple_stats'))

        ccp = pl[CastColumns]

        headers = [ c.source_header for c in ccp.source.source_table.columns]

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

