# -*- coding: utf-8 -*-
import ambry.bundle

from ambry.bundle.events import before_run, before_ingest

class Bundle(ambry.bundle.Bundle):

    @before_run
    def event_before_run(self):
        print 'EVENT BEFORE RUN ', self.identity


    @before_ingest
    def event_before_ingest(self):
        print 'EVENT BEFORE INGEST ', self.identity