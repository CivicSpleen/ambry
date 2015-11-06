# -*- coding: utf-8 -*-

from test.test_base import TestBase
import shutil
import os


class Test(TestBase):

    def setup_temp_dir(self):
        build_url = '/tmp/ambry-build-test'

        try:
            shutil.rmtree(build_url)
        except OSError:
            pass

        os.makedirs(build_url)

        return build_url

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
        from ambry.orm.column import Column

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

        from ambry.orm.column import Column

        c = Column(name='column', sequence_id=1, datatype='int')

        c.transform = 't1|^init|t2|!except|t3|t4'
        self.assertEqual(['init', None], [e['init'] for e in c.expanded_transform])
        self.assertEqual([[int], ['t1', 't2', 't3', 't4']], [e['transforms'] for e in c.expanded_transform])

    def test_col_clean_transform(self):

        b = self.setup_bundle('casters')
        b.sync_in()  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        for t in b.tables:
            for c in t.columns:
                print(c.name, c.transform, c.expanded_transform)

    def test_table_transforms(self):

        d = self.setup_temp_dir()

        b = self.setup_bundle('casters', build_url=d, source_url=d)

        b.sync_in()  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()
        b.ingest()
        b.schema()
        t = b.table('simple_stats')

        row_processors = b.build_caster_code(b.source('simple_stats'))

        rp = row_processors[0]

        # rp(row, row_n, scratch, accumulator, pipe, bundle, source):

        row = [1.0, 1.0, 1.0, 1, 1, 'one', 'two']

        print(rp(row, 0, {}, {}, {}, None, b, b.source('simple_stats')))

    def test_code_calling_pipe(self):

        from ambry.etl import CastColumns

        d = self.setup_temp_dir()
        b = self.setup_bundle('casters', build_url=d, source_url=d)
        b.sync_in()  # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        b.ingest()
        b.schema()

        pl = b.pipeline(source=b.source('simple_stats'))

        ccp = pl[CastColumns]

        source_table = ccp.source.source_table
        dest_table = ccp.source.dest_table

        source_headers = [c.source_header for c in source_table.columns]

        ccp.process_header(source_headers)

        self.assertEquals([1, 2.0, 4.0, 16.0, 1, 1, None, 'ONE', 'TXo', 1, 'Alabama'],
                          ccp.process_body([1.0, 1.0, 1.0, 1, 1, 'one', 'two']))

        self.assertEqual([2, 2.0, 4.0, 16.0, 1, None, 'exception', 'ONE', 'TXo', 1, 'Alabama'],
                         ccp.process_body([1.0, 1.0, 1.0, 1, 'exception', 'one', 'two']))
