# -*- coding: utf-8 -*-
import pytest

from test.test_base import TestBase


class Test(TestBase):

    @pytest.mark.slow
    def test_ingest(self):
        import os
        import glob

        if False:
            # Run this is a persisten directory, so you can review the updated sources.csv, etc.

            for f in glob.glob('/tmp/ingest/*'):
                os.remove(f)

            source_url = '/tmp/ingest'

        else:
            source_url = 'temp://'

        b = self.setup_bundle('complete-load', source_url=source_url)

        b.sync_in()
        b = b.cast_to_subclass()

        sources = None  # 'rent07' # Specify a string to run a single source.

        b.ingest(sources=sources)

        print '====='

        b.ingest(sources=sources)

        print '====='

        b.ingest(sources=sources, force=True)

        b.schema()

        b.sync_out()

        self.assertEquals(5, len(b.tables))

        for t in b.source_tables:
            for c in t.columns:
                self.assertFalse(c.name.startswith('col'))  # colX names indicate no header was found.
