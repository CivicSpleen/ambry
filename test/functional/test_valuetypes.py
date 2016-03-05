# -*- coding: utf-8 -*-

from ambry.orm.column import Column

from test.proto import TestBase


class Test(TestBase):

    def test_basic(self):

        from ambry.valuetype.usps import StateAbr

        sa = StateAbr('AZ')
        self.assertEqual('AZ', sa)
        self.assertEqual(4, sa.fips)
        self.assertEqual('Arizona', sa.name)

        # Convert to a FIPS code
        self.assertEqual('04', sa.fips.str)
        self.assertEqual('04000US04', str(sa.fips.geoid))
        self.assertEqual('04', str(sa.fips.tiger))
        self.assertEqual('0E04', str(sa.fips.gvid))
        self.assertEqual('Arizona', sa.fips.name)
        self.assertEqual('AZ', sa.fips.usps.fips.usps)

        from ambry.valuetype.census import AcsGeoid

        g = AcsGeoid('15000US530330018003')

        self.assertEqual('Washington', g.state.name)
        self.assertEqual('WA', g.state.usps)

    def test_clean_transform(self):
        from ambry.dbexceptions import ConfigurationError

        ct = Column.clean_transform

        self.assertEqual('^init|t1|t2|t3|t4|!except',
                         ct('!except|t1|t2|t3|t4|^init'))

        self.assertEqual('^init|t1|t2|t3|t4|!except||t1|t2|t3|t4|!except',
                         ct('t1|^init|t2|!except|t3|t4||t1|t2|!except|t3|t4'))

        self.assertEqual('^init|t1|t2|t3|t4|!except||t4',
                         ct('t1|^init|t2|!except|t3|t4||t4'))

        self.assertEqual('^init|t1|t2|t3|t4|!except',
                         ct('t1|^init|t2|!except|t3|t4||||'))

        self.assertEqual('^init|t1|t2|t3|t4|!except',
                         ct('|t1|^init|t2|!except|t3|t4||||'))

        self.assertEqual('^init', ct('^init'))

        self.assertEqual('!except', ct('!except'))

        self.assertEqual(ct('||transform2'), '||transform2')

        with self.assertRaises(ConfigurationError):  # Init in second  segment
            ct('t1|^init|t2|!except|t3|t4||t1|^init|t2|!except|t3|t4')

        with self.assertRaises(ConfigurationError):  # Two excepts in a segment
            ct('t1|^init|t2|!except|t3|t4||!except1|!except2')

        with self.assertRaises(ConfigurationError):  # Two inits in a segment
            ct('t1|^init|t2|^init|!except|t3|t4')

        c = Column(name='column', sequence_id=1, datatype='int')

        c.transform = 't1|^init|t2|!except|t3|t4'

        self.assertEqual(['init'], [e['init'] for e in c.expanded_transform])
        self.assertEqual([['t1', 't2', 't3', 't4']], [e['transforms'] for e in c.expanded_transform])

    def test_code_calling_pipe(self):

        from ambry.etl import CastColumns

        b = self.import_single_bundle('build.example.com/casters')
        b.sync_in(force=True)  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        b.ingest()
        b.source_schema()
        b.commit()

        pl = b.pipeline(source=b.source('simple_stats'))

        ccp = pl[CastColumns]

        source_table = ccp.source.source_table

        source_headers = [c.source_header for c in source_table.columns]

        self.assertTrue(len(source_headers) > 0)

        ccp.process_header(source_headers)

        self.assertEquals([1, 2.0, 4.0, 16.0, 1, 1, None, 'ONE', 'TXo', 1, 'Alabama'],
                          ccp.process_body([1.0, 1.0, 1.0, 1, 1, 'one', 'two']))

        self.assertEqual([2, 2.0, 4.0, 16.0, 1, None, 'exception', 'ONE', 'TXo', 1, 'Alabama'],
                         ccp.process_body([1.0, 1.0, 1.0, 1, 'exception', 'one', 'two']))

    def test_classification(self):

        b = self.import_single_bundle('build.example.com/classification')

        b.sync_in()

        s = b.source('classify')

        pl = b.pipeline(s)

        print b.build_caster_code(s, s.headers, pipe=pl)
        print b.build_fs

    def test_raceeth(self):

        from ambry.valuetype import RaceEthHCI, RaceEthReidVT

    def test_time(self):

        from ambry.valuetype import IntervalYearVT, IntervalYearRangeVT, IntervalIsoVT
        from ambry.valuetype import DateValue, TimeValue

        self.assertEqual(2000, IntervalYearVT('2000'))

        self.assertFalse(bool(IntervalYearVT('2000-2001')))

        self.assertEqual('2000/2001', str(IntervalYearRangeVT('2000-2001')))
        self.assertEqual(2000, IntervalYearRangeVT('2000-2001').start)
        self.assertEqual(2001, IntervalYearRangeVT('2000-2001').end)

        self.assertEqual('2000/2001', str(IntervalYearRangeVT('2000','2001')))

        self.assertEqual('1981-04-05/1981-03-06',str(IntervalIsoVT('P1M/1981-04-05')))

        self.assertEquals(4, DateValue('1981-04-05').month)

        self.assertEquals(34,TimeValue('12:34').minute)

    def test_geo(self):

        from ambry.valuetype import GeoCensusVT, GeoAcsVT, GeoGvidVT
        from geoid import acs, civick

        # Check the ACS Geoid directly
        self.assertEqual('California', acs.State(6).geo_name)
        self.assertEqual('San Diego County, California', acs.County(6,73).geo_name)
        self.assertEqual('place in California', acs.Place(6,2980).geo_name)

        # THen check via parsing through the GeoAcsVT
        self.assertEqual('California', GeoAcsVT(str(acs.State(6))).geo_name)
        self.assertEqual('San Diego County, California', GeoAcsVT(str(acs.County(6, 73))).geo_name)
        self.assertEqual('place in California', GeoAcsVT(str(acs.Place(6, 2980))).geo_name)


    def test_measures(self):

        from ambry.valuetype import resolve_value_type, StandardErrorVT


        print resolve_value_type('e/ci')

        t = resolve_value_type('e/ci/u/95')
        v = t(12.34)
        print v
        print v.vt_code

        t = resolve_value_type('e/m/90')

        v = t(12.34)
        print v
        print v.vt_code

        print StandardErrorVT.__doc__

        v = resolve_value_type('e/se')(10)
        print v
        print v.m90 * 1
        print v.m95 * 1
        print v.m99 * 1


        print v.m90.se
        print v.m95.se
        print v.m99.se

