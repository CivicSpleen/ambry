# -*- coding: utf-8 -*-
# Bundle test code

# -*- coding: utf-8 -*-
# Bundle test code

from ambry.bundle.events import *
from ambry.bundle.test import BundleTest

class Test(BundleTest):


    @before_run
    def test_before_run(self):
        print 'BEFORE RUN ', self.bundle.identity

    @after_run
    def test_after_run(self):
        print 'AFTER RUN ', self.bundle.identity

        self.assertEquals(sorted([u'ingest.example.com-stages-stage2', u'ingest.example.com-stages-stage1']),
                          sorted([p.name for p in self.bundle.partitions]))

        p = self.bundle.partition('ingest.example.com-stages-stage1')
        info = p.datafile.info
        self.assertEquals(20, info['rows'])
        self.assertEquals(6, info['cols'])

        p = self.bundle.partition('ingest.example.com-stages-stage2')
        info = p.datafile.info
        self.assertEquals(20, info['rows'])
        self.assertEquals(6, info['cols'])

        self.assertEquals(233310, sum(row.sum for row in p))

    @before_stage(stage=1)
    def test_before_stage_1(self):
        print 'BEFORE STAGE 1', self.bundle.identity

    @after_stage(stage=1)
    def test_after_stage_1(self):
        print 'AFTER STAGE 1', self.bundle.identity

    @before_stage(stage=2)
    def test_before_stage_2(self):
        print 'BEFORE STAGE 2', self.bundle.identity

    @after_stage(stage=2)
    def test_after_stage_2(self):
        print 'AFTER STAGE 2', self.bundle.identity

        self.assertIn('stage2', [t.name for t in self.bundle.dataset.source_tables])

        self.assertEquals([u'id', u'ones', u'tens', u'hundreds', u'thousands', u'sum'],
                          [c.source_header for c in self.bundle.source_table('stage2').columns])
    # Ingest

    @before_ingest(stage=1)
    def test_before_ingest_stage_1(self):
        print 'BEFORE INGEST STAGE 1', self.bundle.identity
        pass

    @after_ingest(stage=1)
    def test_after_ingest_stage_1(self):
        print 'AFTER INGEST STAGE 1', self.bundle.identity

    @after_ingest(stage=2)
    def test_after_ingest_stage_2(self):
        print 'AFTER INGEST STAGE 2', self.bundle.identity


    @after_sourceschema()
    def test_after_sourceschema(self):
        print 'AFTER SOURCE SCHEMA', self.bundle.identity

        self.assertEquals([u'ones', u'tens', u'hundreds', u'thousands', u'sum'],
                          [c.source_header for c in self.bundle.source_table('stage1').columns])

    @after_ingest(stage=2)
    def test_after_ingest_stage_2(self):
        print 'AFTER INGEST STAGE 2', self.bundle.identity

    # Build

    @before_build(stage=1)
    def test_before_build_stage_1(self):
        print 'BEFORE BUILD STAGE 1', self.bundle.identity

        self.assertIn('stage1', [t.name for t in self.bundle.dataset.source_tables])


    @after_build(stage=1)
    def test_after_build_stage_1(self):
        print 'AFTER BUILD STAGE 1', self.bundle.identity

    @before_build(stage=2)
    def test_before_build_stage_2(self):
        print 'BEFORE BUILD STAGE 2', self.bundle.identity

    @after_build(stage=2)
    def test_after_build_stage_2(self):
        print 'AFTER BUILD STAGE 2', self.bundle.identity
