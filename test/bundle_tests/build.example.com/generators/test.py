# -*- coding: utf-8 -*-
# Bundle test code

from ambry.bundle.test import *

class Test(BundleTest):

    @after_ingest()
    def test_after_ingest(self):

        self.assertEquals(['demo'], [t.name for t in self.bundle.dataset.source_tables])

        headers = [u'uuid', u'int', u'float', u'categorical', u'ordinal', u'gaussian',
                   u'triangle', u'exponential', u'year', u'date', u'bg_gvid']

        # try out different styles of calls to the assert
        self.assertInSourceHeaders('demo','uuid')
        self.assertInSourceHeaders('demo', ['int','float'])
        self.assertInSourceHeaders('demo', headers)

    @before_schema()
    def test_before_ingest(self):
        from ambry.orm.exc import NotFoundError

        with self.assertRaises(NotFoundError):
            self.bundle.table('demo')

    @after_schema()
    def test_after_ingest(self):
        headers = [u'uuid', u'int', u'float', u'categorical', u'ordinal', u'gaussian',
                   u'triangle', u'exponential', u'year', u'date', u'bg_gvid']

        self.assertInDestHeaders('demo', headers)

    @after_build()
    def test_after_build(self):

        p = self.bundle.partition(table='demo')

        self.assertTrue(30 < p.stats_dict['float'].mean < 60)
        self.assertEqual(16, len(p.space_coverage))
        self.assertEqual([2000, 2001, 2002, 2003],  p.time_coverage)
        self.assertEqual(['2qZZZZZZZZZ'], p.grain_coverage)

        self.assertEqual(198000,sum(row.int for row in p))
