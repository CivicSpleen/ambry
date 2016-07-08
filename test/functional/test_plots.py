# -*- coding: utf-8 -*-

from ambry.orm.column import Column

from test.proto import TestBase


class Test(TestBase):

    def test_basic(self):
        from ambry.orm.exc import NotFoundError

        if False:
            try:
                self._proto.remove('build.example.com-plot')
            except NotFoundError:
                pass

        l = self.library()
        print 'Library', l.dsn

        b = l.bundle('build.example.com-plot')

        p = b.partition(table='plotdata')

        md = p.measuredim

        for pc in md.primary_dimensions:
            print pc.name, pc.role, pc.valuetype, pc.cardinality

        df = md.dataframe('gauss','group','color', filters={'two_codes': 'AC', 'side': 'right'})


        for r in df.rows:
            print r

        print df.labels
        print df.filtered
        print df.floating

        print df.set

        return

        print '-----'

        for e in md.enumerate_dimension_sets():
            df = md.dataframe('gauss',**e)
            if df is not None:
                print len(df), e
            else:
                print "E", e


        #import json
        #print json.dumps(md.dict, indent=4)
