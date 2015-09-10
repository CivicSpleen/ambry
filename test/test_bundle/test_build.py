# -*- coding: utf-8 -*-

import unittest

from boto.exception import S3ResponseError

from test.test_base import TestBase


class Test(TestBase):

    def test_simple_build(self):
        """Just check that it doesn't throw an exception"""
        from ambry.orm.database import Database
        import shutil
        import os

        build_url = '/tmp/simple'
        shutil.rmtree(build_url)
        os.makedirs(build_url)

        b = self.setup_bundle('simple', build_url = build_url)
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

        for c in p.table.columns:
            print c.name, sd[c.name]

    def test_simple_build_types(self):
        """Build the simple bundle and check that the data types are correct"""

        b = self.setup_bundle('simple')
        b.run()
        l = b.library

        p = list(b.partitions)[0]

        row = p.stream().next()

        for c, v in zip(p.table.columns, row.row):
            if type(v) != unicode:  # It gets reported as string
                self.assertEquals(type(v), c.python_type)

    def test_complete_build(self):
        """Build the simple bundle"""

        from geoid import civick, census

        b = self.setup_bundle('complete-build')
        b.sync_in()
        b = b.cast_to_subclass()
        m = b.import_lib()

        import sys

        RandomSourcePipe = m.__dict__['RandomSourcePipe']

        # 6000 rows and one header
        self.assertEqual(6001, len(list(RandomSourcePipe(b))))

        self.assertEquals('new', b.state)
        self.assertTrue(b.meta())

        self.assertTrue(b.build())

        for p in b.partitions:
            print p.name
            self.assertIn(int(p.identity.time), p.time_coverage)

        self.assertEquals([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                          b.dataset.partitions[0].space_coverage)
        self.assertEquals(u'2qZZZZZZZZZ', b.dataset.partitions[0].grain_coverage[0])

        self.assertEquals([u'0O0001', u'0O0002', u'0O0003', u'0O0101', u'0O0102', u'0O0103'],
                          b.dataset.partitions[2].space_coverage)
        self.assertEquals([u'2qZZZZZZZZZ'], b.dataset.partitions[2].grain_coverage)

        self.assertEqual(4, len(b.dataset.partitions))
        self.assertEqual(2, len(b.dataset.tables))

        print 'Build, testing reads'

        p = list(b.partitions)[0]

        print p.datafile.reader.info

        self.assertEquals(6000, sum(1 for row in p.datafile.reader))

        self.assertEquals(48, len(b.dataset.stats))

        self.assertEquals('build_done', b.state)

    def test_complete_load(self):
        """Build the complete-load"""

        b = self.setup_bundle('complete-load')
        b.sync_in()
        b = b.cast_to_subclass()
        b.ingest()
