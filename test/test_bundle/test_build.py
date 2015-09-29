# -*- coding: utf-8 -*-

import unittest

from boto.exception import S3ResponseError

from test.test_base import TestBase
import shutil
import os


class Test(TestBase):

    def setup_temp_dir(self):
        build_url = '/tmp/ambry-build-test'
        if not os.path.exists(build_url):
            os.makedirs(build_url)
        shutil.rmtree(build_url)
        os.makedirs(build_url)

        return build_url

    def test_simple_build(self):
        """Just check that it doesn't throw an exception"""
        from ambry.orm.database import Database

        b = self.setup_bundle('simple', build_url=self.setup_temp_dir())
        l = b._library
        b.sync_in()

        b.ingest()

        self.assertEquals(1, len(b.source_tables))
        self.assertEquals(0, len(b.tables))

        b.schema()

        self.assertEquals(1, len(b.tables))

        b.build()

        p = list(b.partitions)[0]
        sd = p.stats_dict

        self.assertEqual(0, round(sd['float']['min']))
        self.assertEqual(100, round(sd['float']['max']))
        self.assertEqual(50, round(sd['float']['mean']))
        self.assertEqual(50, round(sd['float']['p50']))

        with p.datafile.reader as r:
            self.assertEqual(50, round(sum(row.float for row in p.datafile.reader) / float(r.n_rows)))

        row = p.stream().next()

        # Check that the cells of the first row all have the right type.
        for c, v in zip(p.table.columns, row.row):
            if type(v) != unicode:  # It gets reported as string
                self.assertEquals(type(v), c.python_type)

    def test_complete_build(self):
        """Build the simple bundle"""
        from ambry.etl import GeneratorSourcePipe

        from geoid import civick, census

        b = self.setup_bundle('complete-build', build_url=self.setup_temp_dir())
        b.sync_in()
        b = b.cast_to_subclass()

        RandomSourcePipe = b.import_lib().__dict__['RandomSourcePipe']

        # 6000 rows and one header
        self.assertEqual(6001, len(list(RandomSourcePipe(b))))

        self.assertEquals('new', b.state)
        b.ingest()

        self.assertEquals(6000, sum(1 for row in b.source('source1').datafile.reader))

        self.assertEquals(1, len(b.source_tables))

        self.assertTrue(b.schema())

        self.assertEquals(1, len(b.source_tables))

        self.assertTrue(b.build())

        for p in b.partitions:

            self.assertIn(int(p.identity.time), p.time_coverage)

        self.assertEquals([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                          b.dataset.partitions[0].space_coverage)
        self.assertEquals(u'2qZZZZZZZZZZ', b.dataset.partitions[0].grain_coverage[0])

        self.assertEquals([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                          b.dataset.partitions[2].space_coverage)
        self.assertEquals([u'2qZZZZZZZZZZ'], b.dataset.partitions[2].grain_coverage)

        self.assertEqual(4, len(b.dataset.partitions))
        self.assertEqual(2, len(b.dataset.tables))

        print 'Build, testing reads'

        p = list(b.partitions)[0]

        self.assertEquals(6000, sum(1 for row in p.datafile.reader))

        self.assertEquals(48, len(b.dataset.stats))

        self.assertEquals('build_done', b.state)

    def test_complete_build_run(self):
        """Build the complete-load"""

        b = self.setup_bundle('complete-build', build_url=self.setup_temp_dir())
        b.sync_in()
        b = b.cast_to_subclass()
        b.run()

    def test_complete_load(self):
        """Build the complete-load"""

        b = self.setup_bundle('complete-load', build_url=self.setup_temp_dir())
        b.sync_in()
        b = b.cast_to_subclass()
        b.run()

    def test_dimensions(self):
        """Build the complete-load"""

        build_url = '/tmp/ambry-build-test'
        if not os.path.exists(build_url):
            os.makedirs(build_url)
        shutil.rmtree(build_url)
        os.makedirs(build_url)

        b = self.setup_bundle('dimensions', build_url=self.setup_temp_dir())
        b.sync_in()
        b = b.cast_to_subclass()
        b.run()

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

    def test_complete_ref(self):
        """Build the complete-load"""

        d =self.setup_temp_dir()

        b = self.setup_bundle('complete-ref', build_url=d, source_url = d)
        b.sync_in()
        b = b.cast_to_subclass()
        b.run()

    def test_casters(self):
        """Build the complete-load"""
        from ambry.dbexceptions import PhaseError

        d = self.setup_temp_dir()

        b = self.setup_bundle('casters', build_url=d, source_url=d)
        b.sync_in()
        b = b.cast_to_subclass()

        try:
            b.run()
        except PhaseError as e:  # Gets cast errors, which are converted to codes
            self.assertEqual(1, len(b.dataset.codes))

        b.commit()
        b.table('simple').column('keptcodes').caster = 'remove_codes'
        b.commit()

        b.dataset.codes[:] = []  # Reset the codes, or the next build will think it had errors.

        try:
            b.build()
        except Exception as exc:
            if exc.message == 'unsupported locale setting':
                raise EnvironmentError('You need to install en_US locale to run that test.')
            else:
                raise

        self.assertEquals(1, len(list(b.partitions)))

        mn = mx = 0
        for row in list(b.partitions)[0].stream():
            self.assertEqual(row['index'], row['index2'])
            int(row['numcom'])  # Check that the comma was removed
            mn, mx = min(mn, row['codes']), max(mx, row['codes'])

        self.assertEqual(-1, mn)  # The '*' should have been turned into a -1
        self.assertEqual(6, mx)

        self.assertEqual(0, len(b.dataset.codes))