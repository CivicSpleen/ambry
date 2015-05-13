"""
Created on Aug 31, 2012

@author: eric
"""
import unittest

from test_base import TestBase  # @UnresolvedImport


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
            from random import randint

            return (x, randint(0, 1000))

        o = f(1)
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
            from random import randint

            return (x, randint(0, 1000))

        o = g(1)
        sleep(2)
        self.assertEquals(o, g(1))
        sleep(4)
        self.assertNotEquals(o, g(1))

        config_str = """
    about:
        groups:
            - Group 1
            - Group 2
        license: license
        rights: rights
        subject: subject
        summary: summary
        tags:
            - Tag1
            - Tag 2
        title: title
        url: url
    build:
        foo: bar
        baz: bingo
    contact:
        creator:
            email: creator.email
            name: creator.name
            url: creator.url
        maintainer:
            email: null
            name: null
            url: null
        publisher:
            email: null
            name: null
            url: null
        source:
            email: source.email
            name: source.name
            url: source.url
    extract: {}
    nonterm:
        a: 1
        b: 2
        c: 3
    identity:
        dataset: dataset
        id: id
        revision: revision
        source: source
        subset: subset
        variation: variation
        version: version
    names:
        fqname: fqname
        name: name
        vid: vid
        vname: vname

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
        yahoo:
            description: The Yahoo Homepage
            url: http://yahoo.com
    """

    def test_metadata(self):
        from ambry.bundle.meta import Metadata, ScalarTerm, TypedDictGroup, \
            VarDictGroup, DictGroup, DictTerm, ListGroup

        import yaml
        from ambry.util import AttrDict

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

        #
        # Dict Group

        tt.group.term = 'Term'
        tt.group.term2 = 'Term2'

        with self.assertRaises(AttributeError):
            tt.group.term3 = 'Term3'

        self.assertEquals('Term', tt.group.term)
        self.assertEquals('Term2', tt.group.term2)
        self.assertEquals('Term', tt.group['term'])
        self.assertEquals(['term', 'term2', 'dterm'], tt.group.keys())
        self.assertEquals(['Term', 'Term2',
                           AttrDict([('dterm1', None), ('unset_term', None),
                                     ('dterm2', None)])], tt.group.values())

        #
        # Dict Term

        tt.group.dterm.dterm1 = 'dterm1'
        tt.group.dterm.dterm2 = 'dterm2'

        with self.assertRaises(AttributeError):
            tt.group.dterm.dterm3 = 'dterm3'

        self.assertEquals('dterm1', tt.group.dterm.dterm1)

        self.assertEquals(['dterm1', 'unset_term', 'dterm2'],
                          tt.group.dterm.keys())
        self.assertEquals(['dterm1', None, 'dterm2'], tt.group.dterm.values())

        # List Group

        tt.lgroup.append({'k1': 'v1'})
        tt.lgroup.append({'k2': 'v2'})

        self.assertEquals('v1', tt.lgroup[0]['k1'])
        self.assertEquals('v2', tt.lgroup[1]['k2'])

        # TypedDictGroup

        tt.tdgroup.foo.dterm1 = 'foo.dterm1'

        self.assertEqual('foo.dterm1', tt.tdgroup.foo.dterm1)

        tt.tdgroup.foo.dterm2 = 'foo.dterm2'
        tt.tdgroup.baz.dterm1 = 'foo.dterm1'

        # VarDict Group

        tt.vdgroup.k1['v1'] = 'v1'
        tt.vdgroup.k1.v2 = 'v2'

        d = dict(
            about=dict(
                title='title',
                subject='subject',
                rights='rights',
                summary='Summary',
                tags='Foobotom'
            ),
            contact=dict(
                creator=dict(
                    name='Name',
                    email='Email',
                    bingo='bingo'
                )
            ),
            # These are note part of the defined set, so aren't converted to terms
            build=dict(
                foo='foo',
                bar='bar'
            ),
            partitions=[
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

        t2 = Top()

        self.assertIn(('contact', 'creator', 'bingo'), top.errors)

        self.assertIn('publisher', top.contact.keys())
        self.assertIn('url', dict(top.contact.creator))
        self.assertEqual('Email', top.contact.creator.email)

        self.assertIn('name', top.partitions[0])
        self.assertNotIn('space', top.partitions[0])
        self.assertIn('space', top.partitions[2])
        self.assertEquals('foo', top.partitions[0]['name'])

        top.sources.foo.url = 'url'
        top.sources.bar.description = 'description'

        top = Top(yaml.load(config_str))

        self.assertEquals(['foo', 'baz'], top.build.keys())
        self.assertEquals('bar', top.build.foo)
        self.assertEquals('bar', top.build['foo'])

        self.assertEqual(6, len(top.partitions))

        self.assertIn('google', top.sources.keys())
        self.assertIn('yahoo', top.sources.keys())
        self.assertEquals('http://yahoo.com', top.sources.yahoo.url)

        # print top.write_to_dir(None)

        # for (group, term, subterm),v in top.rows:
        # print group, term, subterm,v

        t3 = Top()
        t3.load_rows(top.rows)

        # print t3.dump()

    def test_metadata_TypedDictGroup(self):
        from ambry.util.meta import Metadata, ScalarTerm, TypedDictGroup, \
            DictTerm

        import yaml

        class TestDictTerm(DictTerm):
            dterm1 = ScalarTerm()
            dterm2 = ScalarTerm()
            unset_term = ScalarTerm()

        class TestTDGroup(TypedDictGroup):
            _proto = TestDictTerm()

        class TestTop(Metadata):
            group = TestTDGroup()

        config_str = """
group:
    item1:
        dterm1: dterm1
        dterm2: dterm2
"""

        result_str = (config_str + "        unset_term: null").strip("\n")

        top = TestTop(yaml.load(config_str))

        self.assertEquals(result_str, top.dump().strip('\n'))

        self.assertEquals('dterm1', top.group.item1.dterm1)
        self.assertEquals('dterm1', top.group.item1['dterm1'])


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite


if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())