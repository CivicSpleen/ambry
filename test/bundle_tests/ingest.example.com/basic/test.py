# -*- coding: utf-8 -*-
# Bundle test code

from ambry.bundle.test import BundleTest, after_ingest, before_ingest

class Test(BundleTest):

    @before_ingest()
    def test_before_ingest(self):
        print 'BEFORE INGEST', self.bundle.identity

    @before_ingest()
    def test_before_ingest(self):
        self.assertTrue(False)

    @after_ingest()
    def test_after_ingest(self):
        print 'AFTER INGEST', self.bundle.identity
        x = 1/0


    def x_test_do_ingest(self):
        """Test ingestion, for when the ingestion hasn't been done yet. """

        bundle.clean_ingested()
        bundle.ingest()

        self.test_ingested()

        bundle.clean_ingested()


    def x_test_ingested(self):
        """Run tests on ingestion, when the ingestion is done prior to running the test"""
        sources = [s for s in bundle.sources if s.is_downloadable]

        self.assertEqual(1, len(sources))
        s = sources[0]
        self.assertEqual(s.STATES.INGESTED, s.state)
        self.assertTrue(s.datafile.exists)

        sum_ = sum(float(row.float) for row in s.datafile)
        self.assertEquals(497055.0, round(sum_, 0))

        d =  s.datafile.info
        self.assertEquals(1, d['data_start_row'])
        self.assertEquals(10001, d['data_end_row'])
        self.assertEquals(10001, d['rows'])
        self.assertEquals(4, d['cols'])
        self.assertEquals([u'id', u'uuid', u'int', u'float'], d['headers'])
        self.assertEquals([0], d['header_rows'])

