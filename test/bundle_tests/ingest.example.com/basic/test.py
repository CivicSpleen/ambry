# -*- coding: utf-8 -*-
# Bundle test code

import unittest

# noinspection PyUnresolvedReferences
from ambry.build import bundle  # Set in Bundle.run_tests
# noinspection PyUnresolvedReferences
from ambry.build import library  # Set in Bundle.run_tests

from ambry.bundle import Bundle

class Test(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @unittest.skipIf(bundle.state == 'clean_done', 'Only test when clean')
    def test_if_clean(self):
        print "test_if_clean", bundle.state

    @unittest.skipIf(bundle.state != 'clean_done', 'Only test if not clean')
    def test_if_not_clean(self):
        print "test_if_not_clean", bundle.state

    @unittest.skipIf(bundle.state in (Bundle.STATES.INGESTED,Bundle.STATES.FINALIZED,Bundle.STATES.BUILT) ,
                     'State is ingested, built or finalized')
    def test_do_ingest(self):
        """Test ingestion, for when the ingestion hasn't been done yet. """
        bundle.clean_ingested()
        bundle.ingest()

        self.test_ingested()

        bundle.clean_ingested()

    @unittest.skipIf(bundle.state != Bundle.STATES.INGESTED, 'Bundle is not ingested')
    def test_ingested(self):
        """Run tests on ingestion, when the ingestion is done prior to running the test"""
        sources = [s for s in bundle.sources if s.is_downloadable]

        self.assertEqual(1, len(sources))
        s = sources[0]
        self.assertEqual(s.STATES.INGESTED, s.state)
        self.assertTrue(s.datafile.exists)

        sum_ = sum(float(row.float) for row in s.datafile)
        self.assertEquals(497055.0, round(sum_, 0))
