# -*- coding: utf-8 -*-

from test.test_base import TestBase
import os
import shutil

import pytest


class Test(TestBase):

    def setup_temp_dir(self):
        build_url = '/tmp/ambry-build-test'

        try:
            shutil.rmtree(build_url)
        except OSError:

            pass

        os.makedirs(build_url)

        return build_url

    @pytest.mark.slow
    def test_simple_build(self):
        """Just check that it doesn't throw an exception"""

        b = self.setup_bundle('simple', build_url=self.setup_temp_dir())
        b.sync_in()

        b.ingest()

        for t in b.source_tables:
            for c in t.columns:
                print 'ST', t.name, c.name, c.datatype

        self.assertEqual(1, len(b.source_tables))
        self.assertEqual(0, len(b.tables))

        b.schema()
        #b.build_schema()

        for t in b.tables:
            for c in t.columns:
                print "SB", t.name, c.name, c.datatype

        b.commit()

        self.assertEquals(1, len(b.tables))

        b.build()

        p = next(iter((b.partitions)))

        sd = p.stats_dict

        print sd['id']['count']
        print sd['float']['lom'], sd['float']['min']
        self.assertEqual(0, round(sd['float']['min']))
        self.assertEqual(100, round(sd['float']['max']))
        self.assertEqual(50, round(sd['float']['mean']))
        self.assertEqual(50, round(sd['float']['p50']))

        with p.datafile.reader as r:
            self.assertEqual(50, round(sum(row.float for row in p.datafile.reader) / float(r.n_rows)))

        row = next(iter(p))

        # Check that the cells of the first row all have the right type.
        for c, v in zip(p.table.columns, row.row):
            if type(v) != unicode:  # It gets reported as string
                self.assertEqual(type(v), c.python_type)

    @pytest.mark.slow
    def test_complete_build(self):
        """Build the simple bundle"""

        b = self.setup_bundle('complete-build', build_url=self.setup_temp_dir())
        b.sync_in()
        b = b.cast_to_subclass()

        RandomSourcePipe = b.import_lib().__dict__['RandomSourcePipe']

        # 6000 rows and one header
        self.assertEqual(6001, len(list(RandomSourcePipe(b))))

        self.assertEqual('new', b.state)
        b.ingest()

        self.assertEqual(6000, sum(1 for row in b.source('source1').datafile.reader))

        self.assertEqual(1, len(b.source_tables))

        self.assertTrue(b.schema())

        self.assertEqual(1, len(b.source_tables))

        self.assertTrue(b.build())

        for p in b.partitions:

            self.assertIn(int(p.identity.time), p.time_coverage)

        self.assertEqual([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                         b.dataset.partitions[0].space_coverage)
        self.assertEqual(u'2qZZZZZZZZZZ', b.dataset.partitions[0].grain_coverage[0])

        self.assertEqual([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                         b.dataset.partitions[2].space_coverage)
        self.assertEqual([u'2qZZZZZZZZZZ'], b.dataset.partitions[2].grain_coverage)

        self.assertEqual(4, len(b.dataset.partitions))
        self.assertEqual(2, len(b.dataset.tables))

        print 'Build, testing reads'

        p = list(b.partitions)[0]

        self.assertEqual(6000, sum(1 for row in p.datafile.reader))

        self.assertEqual(48, len(b.dataset.stats))

        self.assertEqual('build_done', b.state)

    @pytest.mark.slow
    def test_complete_build_run(self):
        """Build the complete-load"""
        b = self.setup_bundle('complete-build', build_url=self.setup_temp_dir())
        b.sync_in()
        b = b.cast_to_subclass()
        b.run()

    @pytest.mark.slow
    def test_complete_load(self):
        """Build the complete-load"""
        b = self.setup_bundle('complete-load', build_url=self.setup_temp_dir())
        b.sync_in()
        b = b.cast_to_subclass()
        b.run()

    @pytest.mark.slow
    def test_dimensions(self):
        """Test a simple bundle which has  custom datatypes and derivations in the schema. """

        from itertools import islice

        build_url = '/tmp/ambry-build-test'
        if not os.path.exists(build_url):
            os.makedirs(build_url)
        shutil.rmtree(build_url)
        os.makedirs(build_url)

        b = self.setup_bundle('dimensions', build_url=self.setup_temp_dir())
        b.sync_in()
        b = b.cast_to_subclass()

        self.assertTrue(b.ingest())
        self.assertTrue(b.schema())
        self.assertTrue(b.build())

        p = next(iter(b.partitions))

        with p.datafile.reader as r:
            row =  next(islice(r.select(lambda r: r.state == 6), None, 1))

        self.assertEqual('Alameda County, California', row['name'])
        self.assertEqual('05000US06001', row['geoid'])
        self.assertEqual('0O0601', row['geoid2'])
        self.assertEqual('05000US06001', row['geoid3'])
        self.assertEqual(600, row['percent'])


        with b.partition(table='counties').datafile.reader as r:
            for row in r:
                print row.row


    @pytest.mark.slow
    def test_generators(self):
        """Build the complete-load"""

        build_url = '/tmp/ambry-build-test'
        if not os.path.exists(build_url):
            os.makedirs(build_url)
        shutil.rmtree(build_url)
        os.makedirs(build_url)

        b = self.setup_bundle('generators', build_url=self.setup_temp_dir())
        b.sync_in()
        b = b.cast_to_subclass()
        b.run()

    @pytest.mark.slow
    def test_complete_ref(self):
        """Build the complete-load"""

        d = self.setup_temp_dir()

        b = self.setup_bundle('complete-ref', build_url=d, source_url=d)
        b.sync_in()
        b = b.cast_to_subclass()

        b.run_stages()

        with b.partition('example.com-complete-ref-stage2').datafile.reader as r:
            for row in r:
                self.assertEqual(row.tens, row.stage1_tens_a)

    @pytest.mark.slow
    def test_casters(self):
        """Build the complete-load"""
        from ambry.dbexceptions import PhaseError

        d = self.setup_temp_dir()
        b = self.setup_bundle('casters', build_url=d, source_url=d)
        b.sync_in(); # Required to get bundle for cast_to_subclass to work.
        b = b.cast_to_subclass()

        try:
            b.run()
        except PhaseError as e:  # Gets cast errors, which are converted to codes
            self.assertEqual(1, len(b.dataset.codes))

        self.assertEqual(3, len(list(b.partitions)))

        p = b.partition(table='simple')

        mn = mx = 0
        for row in p:
            self.assertEqual(row['index'], row['index2'])
            int(row['numcom'])  # Check that the comma was removed
            mn, mx = min(mn, row['removecodes']), max(mx, row['removecodes'])

        self.assertEqual(-1, mn)  # The '*' should have been turned into a -1
        self.assertEqual(4, mx)

        self.assertEqual(0, len(b.dataset.codes))

        # Varitions in the function signatire for casters

        with b.partition(table='integers').datafile.reader as r:
            for row in r:

                self.assertEqual(row.id, row.a)
                self.assertEqual(row.id * 2, row.b)
                self.assertEqual(row.id * 2, row.c)
                self.assertEqual(row.id * 2, row.d)
                self.assertEqual(row.id * 3, row.e)
                self.assertEqual(8, row.f)