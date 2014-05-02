"""
Created on Aug 31, 2012

@author: eric
"""
import unittest
from test_base import  TestBase  # @UnresolvedImport

class Test(TestBase):

    def setUp(self):
        self.yaml_config = """
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
    baz: bingo
    foo: bar
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
nonterm:
    a: 1
    b: 2
    c: 3
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

    def tearDown(self):
        pass

    def test_basic(self):
        from ambry.bundle.meta import Metadata, ScalarTerm, TypedDictGroup, VarDictGroup, DictGroup, DictTerm, ListGroup
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


        ##
        ## Dict Group

        tt.group.term = 'Term'
        tt.group.term2 = 'Term2'

        with self.assertRaises(AttributeError):
            tt.group.term3 = 'Term3'

        self.assertEquals('Term', tt.group.term)
        self.assertEquals('Term2', tt.group.term2)
        self.assertEquals('Term', tt.group['term'])
        self.assertEquals(['term', 'term2', 'dterm'], tt.group.keys())
        self.assertEquals(['Term', 'Term2',
                           AttrDict([('dterm1', None), ('unset_term', None), ('dterm2', None)])], tt.group.values())

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

        tt.lgroup.append({'k1': 'v1'})
        tt.lgroup.append({'k2': 'v2'})

        self.assertEquals('v1', tt.lgroup[0]['k1'])
        self.assertEquals('v2', tt.lgroup[1]['k2'])

        ## TypedDictGroup

        tt.tdgroup.foo.dterm1 = 'foo.dterm1'

        self.assertEqual('foo.dterm1', tt.tdgroup.foo.dterm1)

        tt.tdgroup.foo.dterm2 = 'foo.dterm2'
        tt.tdgroup.baz.dterm1 = 'foo.dterm1'

        ## VarDict Group

        tt.vdgroup.k1['v1'] = 'v1'
        tt.vdgroup.k1.v2 = 'v2'


    def test_metadata(self):
        from ambry.bundle.meta import Top

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

        self.assertIn(('contact', 'creator', 'bingo'), top.errors)

        self.assertIn('publisher', top.contact.keys())
        self.assertIn('url', dict(top.contact.creator))
        self.assertEqual('Email',top.contact.creator.email)

        self.assertIn('name', top.partitions[0])
        self.assertNotIn('space', top.partitions[0])
        self.assertIn('space', top.partitions[2])
        self.assertEquals('foo', top.partitions[0]['name'])

        top.sources.foo.url = 'url'
        top.sources.bar.description = 'description'


    def test_yaml(self):
        import yaml
        from ambry.bundle.meta import Top

        t1 = Top(yaml.load(self.yaml_config))

        self.assertEquals(['foo', 'baz'], t1.build.keys())
        self.assertEquals('bar', t1.build.foo)
        self.assertEquals('bar', t1.build['foo'])

        self.assertEqual(6, len(t1.partitions))

        self.assertIn('google',t1.sources.keys())
        self.assertIn('yahoo', t1.sources.keys())
        self.assertEquals('http://yahoo.com', t1.sources.yahoo.url)

        t2 = Top()
        t2.load_rows(t1.rows)

        self.assertEquals(self.yaml_config.strip(' \n'), t2.dump().strip(' \n'))

    def test_metadata_TypedDictGroup(self):

        from ambry.util.meta import Metadata, ScalarTerm, TypedDictGroup, VarDictGroup, DictGroup, DictTerm, ListTerm, \
            ListGroup

        import pprint, yaml

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


    def test_read_write(self):
        import yaml
        from ambry.bundle.meta import Top

        t1 = Top(yaml.load(self.yaml_config))

        t1.write_to_dir(None)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())