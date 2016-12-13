# -*- coding: utf-8 -*-

from ambry.orm.column import Column

from test.proto import TestBase
import unittest
class Test(TestBase):



    def test_clean_transform(self):
        from ambry.dbexceptions import ConfigurationError

        ct = Column.clean_transform

        self.assertEqual('^init|t1|t2|t3|t4|!except',
                         ct('!except|t1|t2|t3|t4|^init'))

        self.assertEqual('^init|t1|t2|t3|t4|!except;t1|t2|t3|t4|!except',
                         ct('t1|^init|t2|!except|t3|t4;t1|t2|!except|t3|t4'))

        self.assertEqual('^init|t1|t2|t3|t4|!except;t4',
                         ct('t1|^init|t2|!except|t3|t4;t4'))

        self.assertEqual('^init|t1|t2|t3|t4|!except;;',
                         ct('t1|^init|t2|!except|t3|t4;;'))

        self.assertEqual('^init|t1|t2|t3|t4|!except;;',
                         ct('|t1|^init|t2|!except|t3|t4;;'))

        self.assertEqual('^init', ct('^init'))

        self.assertEqual('!except', ct('!except'))

        self.assertEqual(ct(';transform2'), ';transform2')

        with self.assertRaises(ConfigurationError):  # Init in second  segment
            ct('t1|^init|t2|!except|t3|t4;t1|^init|t2|!except|t3|t4')

        with self.assertRaises(ConfigurationError):  # Two excepts in a segment
            ct('t1|^init|t2|!except|t3|t4||!except1|!except2')

        with self.assertRaises(ConfigurationError):  # Two inits in a segment
            ct('t1|^init|t2|^init|!except|t3|t4')

        c = Column(name='column', sequence_id=1, datatype='int')

        c.transform = 't1|^init|t2|!except|t3|t4'

        self.assertEqual(['init'], [e['init'] for e in c.expanded_transform])
        self.assertEqual([['t1', 't2', 't3', 't4']], [e['transforms'] for e in c.expanded_transform])

    @unittest.skip('Broken')
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

        for t in b.tables:
            for c in t.columns:
                print c.name, c.role, c.datatype, c.valuetype

    def test_raceeth(self):

        from ambry.valuetype import RaceEthReidVT, RaceEthCodeHCI

        return

        self.assertEqual(1, RaceEthNameHCI('AIAN').civick)
        self.assertEqual('aian', RaceEthNameHCI('AIAN').civick.name)
        self.assertEqual(6, RaceEthNameHCI('White').civick)
        self.assertEqual('white', RaceEthNameHCI('White').civick.name)

        self.assertEqual('all', RaceEthNameHCI('Total').civick.name)

        self.assertFalse(bool(RaceEthNameHCI(None).civick.name))


    def test_text(self):

        from ambry.valuetype import TextValue, cast_str, NoneValue

        x = cast_str(TextValue(None), 'foobar', {})
        self.assertEqual(None, x)

        print cast_str(TextValue(None), 'foobar', {})

    def test_time(self):

        from ambry.valuetype import  IntervalIsoVT, YearValue, YearRangeValue, resolve_value_type
        from ambry.valuetype import DateValue, TimeValue

        self.assertEqual(2000, YearValue('2000'))

        self.assertFalse(bool(YearValue('2000-2001')))

        self.assertEqual('2000/2001', str(YearRangeValue('2000-2001')))
        self.assertEqual(2000, YearRangeValue('2000-2001').start)
        self.assertEqual(2001, YearRangeValue('2000-2001').end)

        self.assertEqual('2000/2001', str(YearRangeValue('2000/2001')))

        self.assertEqual('1981-04-05/1981-03-06',str(IntervalIsoVT('P1M/1981-04-05')))

        self.assertEquals(4, DateValue('1981-04-05').month)

        self.assertEquals(34,TimeValue('12:34').minute)

        i = resolve_value_type('interval')('2000-2001')
        i.raise_for_error()
        self.assertEquals(2000, i.start.year)
        self.assertEquals(2001, i.end.year)

        i = resolve_value_type('interval')('2000')
        i.raise_for_error()
        self.assertEquals(2000, i.start.year)
        self.assertEquals(2001, i.end.year)

        i = resolve_value_type('interval')(2000)
        i.raise_for_error()
        self.assertEquals(2000, i.start.year)
        self.assertEquals(2001, i.end.year)

        i = resolve_value_type('interval')(' 2000 ')
        i.raise_for_error()
        self.assertEquals(2000, i.start.year)
        self.assertEquals(2001, i.end.year)

        with self.assertRaises(ValueError):
            i = resolve_value_type('interval')(' foobar ')

        i = resolve_value_type('interval')(2010.0)
        i.raise_for_error()
        self.assertEquals(2010, i.start.year)
        self.assertEquals(2011, i.end.year)


    def test_geo(self):

        from ambry.valuetype import GeoCensusVT, GeoAcsVT, GeoGvidVT, resolve_value_type, cast_unicode
        from geoid import acs, civick

        # Check the ACS Geoid directly
        self.assertEqual('California', acs.State(6).geo_name)
        self.assertEqual('San Diego County, California', acs.County(6,73).geo_name)
        self.assertEqual('place in California', acs.Place(6,2980).geo_name)

        # THen check via parsing through the GeoAcsVT
        self.assertEqual('California', GeoAcsVT(str(acs.State(6))).geo_name)
        self.assertEqual('San Diego County, California', GeoAcsVT(str(acs.County(6, 73))).geo_name)
        self.assertEqual('place in California', GeoAcsVT(str(acs.Place(6, 2980))).geo_name)

        self.assertEqual('California', GeoGvidVT('0O0601').state_name)
        self.assertEqual('Alameda County, California',  resolve_value_type('gvid')('0O0601').acs.geo_name)

        # Check that adding a parameter to the vt code will select a new parser.
        cls = resolve_value_type('geoid/census/tract')

        self.assertTrue(bool(cls))

        self.assertEqual(402600, cls('06001402600').tract)

        self.assertEquals('4026.00', cls('06001402600').dotted)

        self.assertEquals('4002.00',cast_unicode(cls('06001400200').dotted, 'tract', {}))

    def test_numbers(self):
        from collections import defaultdict
        from ambry.valuetype import IntDimension, IntValue, FloatValue, FloatDimension, cast_float

        errors = defaultdict(set)

        f = FloatDimension(100.4)
        f.raise_for_error()
        self.assertFalse(f.is_none)
        self.assertEqual(100.4, f)

        self.assertEqual(100.4, cast_float(f, 'test',errors))

        self.assertEqual(0, len(errors))

        f = FloatDimension('')
        f.raise_for_error()
        self.assertTrue(f.is_none)

        print f, type(f)
        print cast_float(f, 'test', errors)
        print cast_float(f, 'test', errors)

        self.assertEqual(0, len(errors))

        f = FloatDimension('a')
        with self.assertRaises(ValueError):
            f.raise_for_error()

        self.assertFalse(f.is_none)

        print f, type(f)
        print cast_float(f, 'test', errors)
        print cast_float(f, 'test', errors)

        print errors

    def test_failures(self):

        from ambry.valuetype import StrValue, upper, NoneValue

        v = StrValue(None)

        self.assertIs(v, NoneValue)

        v = upper(v)

        self.assertIs(v, NoneValue)


    def test_measures_errors(self):

        import ambry.valuetype as vt
        from ambry.valuetype import resolve_value_type

        self.assertEqual('A standard error', vt.StandardErrorVT.__doc__)

        self.assertEqual( vt.ConfidenceIntervalHalfVT, resolve_value_type('ci'))

        # Test on-the-fly classes. The class is returned for e/ci, but it created a new class
        # and the vt_code is set to e/ci/u/95
        t = resolve_value_type('ci/u/95')
        self.assertEqual(vt.ConfidenceIntervalHalfVT, resolve_value_type('ci'))
        self.assertEquals(12.34, float(t(12.34)))
        self.assertEqual('ci/u/95', t(12.34).vt_code)

        t = resolve_value_type('margin/90')
        self.assertEquals(12.34, float(t(12.34)))
        self.assertEqual('margin/90', t(12.34).vt_code)

        self.assertAlmostEqual(10.0, resolve_value_type('margin/90')(16.45).se)
        self.assertAlmostEqual(10.0, resolve_value_type('margin/95')(19.6).se)
        self.assertAlmostEqual(10.0, resolve_value_type('margin/99')(25.75).se)

        # Convert to various margins.
        v = resolve_value_type('se')(10)
        self.assertEqual(10, int(v))
        self.assertEqual(16.45, v.m90 * 1)
        self.assertEqual(19.6, v.m95 * 1)
        self.assertEqual(25.75, v.m99 * 1)

        # Convert to margins and back to se
        self.assertAlmostEqual(10.0, v.m90.se)
        self.assertAlmostEqual(10.0, v.m95.se)
        self.assertAlmostEqual(10.0, v.m95.se)

        print vt.RateVT(0)
        print vt.RateVT(None)

