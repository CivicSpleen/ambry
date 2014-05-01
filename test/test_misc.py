"""
Created on Aug 31, 2012

@author: eric
"""
import unittest
from test_base import  TestBase  # @UnresolvedImport

class Test(TestBase):

    def setUp(self):

        pass

    def tearDown(self):
        pass


    def test_lru(self):
        from ambry.util import lru_cache
        from time import sleep

        @lru_cache(maxsize=3)
        def f(x):
            from  random import randint

            return (x,randint(0,1000))


        o =  f(1)
        self.assertEquals(o, f(1))
        self.assertEquals(o, f(1))
        f(2)
        self.assertEquals(o, f(1))
        f(3)
        f(4)
        f(5)
        self.assertNotEquals(o, f(1))


        #
        # Verify expiration based on time.
        #
        @lru_cache(maxtime=3)
        def g(x):
            from  random import randint

            return (x, randint(0, 1000))

        o = g(1)
        sleep(2)
        self.assertEquals(o, g(1))
        sleep(4)
        self.assertNotEquals(o, g(1))

    def test_metadata(self):
        from ambry.bundle.meta import Top, About, Contact, ContactTerm, PartitionTerm, Partitions
        from ambry.bundle.meta import Metadata, ScalarTerm, TypedDictGroup, VarDictGroup, DictGroup, DictTerm, ListTerm, ListGroup
        from ambry.util import MapView
        import pprint, yaml

        class TestDictTerm(DictTerm):
            dterm1 = ScalarTerm()
            dterm2 = ScalarTerm()
            unset_term = ScalarTerm()

        class TestListGroup(ListGroup):
            _proto = TestDictTerm()

        class TestGroup(DictGroup):
            term = ScalarTerm()
            term2 = ScalarTerm()
            dterm = TestDictTerm()

        class TestTDGroup(TypedDictGroup):
            _proto = TestDictTerm()

        class TestTop(Metadata):
            group = TestGroup()
            tdgroup = TestTDGroup()
            lgroup = TestListGroup()
            vdgroup = VarDictGroup()

        tt = TestTop()

        ##
        ## Dict Group

        tt.group.term = 'Term'
        tt.group.term2 = 'Term2'


        with self.assertRaises(AttributeError):
            tt.group.term3 = 'Term3'


        self.assertEquals('Term',tt.group.term)
        self.assertEquals('Term2', tt.group.term2)
        self.assertEquals('Term', tt.group['term'])
        self.assertEquals(['Term', 'Term2'], tt.group.values())

        ##
        ## Dict Term

        tt.group.dterm.dterm1 = 'dterm1'
        tt.group.dterm.dterm2 = 'dterm2'

        with self.assertRaises(AttributeError):
            tt.group.dterm.dterm3 = 'dterm3'


        self.assertEquals('dterm1', tt.group.dterm.dterm1)

        self.assertEquals(['dterm1', 'unset_term', 'dterm2'], tt.group.dterm.keys())
        self.assertEquals(['dterm1', None, 'dterm2'], tt.group.dterm.values())


        ## List Group

        tt.lgroup.append({'k1':'v1'})
        tt.lgroup.append({'k2': 'v2'})

        self.assertEquals('v1',tt.lgroup[0]['k1'])
        self.assertEquals('v2', tt.lgroup[1]['k2'])

        print '----'

        ## TypedDictGroup

        tt.tdgroup.foo.dterm1 = 'foo.dterm1'
        tt.tdgroup.foo.dterm2 = 'foo.dterm2'
        tt.tdgroup.baz.dterm1 = 'foo.dterm1'

        ## VarDict Group

        tt.vdgroup.k1['v1'] = 'v1'
        tt.vdgroup.k1.v2 = 'v2'

        #pprint.pprint(tt._term_values)

        print tt.dump(map_view=MapView(keys = ('group','lgroup')))

        #print yaml.safe_dump(tt._term_values, default_flow_style=False, indent=4, encoding='utf-8')

        return

        d = dict(
            about = dict(
                title = 'title',
                subject = 'subject',
                rights = 'rights',
                summary = 'Summary',
                tags = 'Foobotom'
            ),
            contact = dict(
                creator = dict(
                    name = 'Name',
                    email = 'Email',
                    bingo = 'bingo'
                )
            ),
            # These are note part of the defined set, so aren't converted to terms
            build = dict(
                foo = 'foo',
                bar = 'bar'
            ),
            partitions = [
                dict(
                    name='foo',
                    grain='bar'
                ),
                dict(
                    time='foo',
                    space='bar'
                ),
                dict(
                    name='name',
                    time='time',
                    space='space',
                    grain='grain',
                    segment=0,

                ),
                ]
        )

        top = Top(d)

        #top.write_to_dir('foobar')

        #pprint.pprint(top.rows)

        t2 = Top()
        #t2.load_rows(top.rows)

        #print yaml.safe_dump(top.dict_by_file(), default_flow_style=False, indent=4, encoding='utf-8')
        print top.contact.value
        print top.contact.creator.value

        return

        self.assertIn(('contact', 'creator', 'bingo'), top.errors)

        self.assertIn('publisher', top.contact.value)
        self.assertIn('url', top.contact.creator.value)
        self.assertEqual('Email',top.contact.creator.email)

        self.assertIn('name', top.partitions[0].value)
        self.assertNotIn('space', top.partitions[0].value)
        self.assertIn('space', top.partitions[2].value)
        self.assertEquals('foo', top.partitions[0].value['name'])

        top.sources.foo.url = 'url'
        top.sources.bar.description = 'description'

        #print top.sources.dict


        config_str = """
partitions:
-   name: source-dataset-subset-variation-tthree
    table: tthree
-   format: geo
    name: source-dataset-subset-variation-geot1-geo
    table: geot1
-   format: geo
    name: source-dataset-subset-variation-geot2-geo
    table: geot2
-   grain: missing
    name: source-dataset-subset-variation-tone-missing
    table: tone
-   name: source-dataset-subset-variation-tone
    table: tone
-   format: csv
    name: source-dataset-subset-variation-csv-csv-1
    segment: 1
    table: csv
sources:
    google:
        description: The Google Homepage
        url: http://google.com
        foo: bar
    yahoo:
        description: The Yahoo Homepage
        url: http://yahoo.com

"""

        import yaml
        import pprint

        d1 = yaml.load(config_str)

        top = Top(d1)
        self.assertEqual(6, len(top.partitions.value))

        parts = top.sources.value

        self.assertIn('google',top.sources.value)
        self.assertIn('yahoo', top.sources.value)

        print top.errors

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())