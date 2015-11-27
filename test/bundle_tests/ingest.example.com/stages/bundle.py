# -*- coding: utf-8 -*-
import ambry.bundle

from ambry.bundle.events import *

class Bundle(ambry.bundle.Bundle):

    @after_run
    def after_run(self):

        for r in self.progress.records:
            print r
            for c in r.children:
                print '    ', c



