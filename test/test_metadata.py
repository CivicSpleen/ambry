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
build:
    baz: bingo
    foo: bar
contact_bundle:
    creator:
        email: creator.email
        name: null
        url: null
    maintainer:
        email: maint.email
        name: null
        url: null
contact_source:
    creator:
        email: null
        name: Source Creator
        url: http://clarinova.com
    maintainer:
        email: null
        name: Source maintainer
        url: http://clarinova.com
dependencies: {}
documentation:
    main: null
    readme: null
extract: {}
identity:
    bspace: null
    btime: null
    dataset: dataset
    id: dxxx
    revision: 3
    source: source
    subset: subset
    type: null
    variation: variation
    version: 1.2.3
names:
    fqname: fqname
    name: name
    vid: vid
    vname: vname
nonterm:
    a: 1
    b: 2
    c: 3
process: {}
sources:
    google:
        description: The Google Homepage
        url: http://google.com
    yahoo:
        description: The Yahoo Homepage
        url: http://yahoo.com
versions:
    3:
        description: d3
        version: s3
views: {}
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


        self.assertEquals('dterm1', tt.group.dterm.dterm1)

        self.assertEquals(['dterm1', 'unset_term', 'dterm2'], tt.group.dterm.keys())
        self.assertEquals(['dterm1', None, 'dterm2'], tt.group.dterm.values())


        ## List Group


        tt.lgroup.append({'dterm1': 'dterm1'})
        tt.lgroup.append({'dterm2': 'dterm2'})

        self.assertEquals('dterm2', tt.lgroup[1]['dterm2'])
        self.assertEquals('dterm1', tt.lgroup[0]['dterm1'])


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
            contact_bundle = dict(
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

        print top.dump()

        print dict(top.partitions[0])

        self.assertIn(('contact_bundle', 'creator', 'bingo'), top.errors)

        self.assertIn('creator', top.contact_bundle.keys())
        self.assertIn('url', dict(top.contact_bundle.creator))
        self.assertEqual('Email',top.contact_bundle.creator.email)

        self.assertIn('name', top.partitions[0])



        self.assertIn('space', top.partitions[2])
        self.assertNotIn('space', top.partitions[0])
        self.assertEquals('foo', top.partitions[0]['name'])

        top.sources.foo.url = 'url'
        top.sources.bar.description = 'description'


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


    def test_rows(self):
        import yaml
        from ambry.bundle.meta import Top

        t1 = Top(yaml.load(self.yaml_config))

        self.assertEquals(['foo', 'baz'], t1.build.keys())
        self.assertEquals('bar', t1.build.foo)
        self.assertEquals('bar', t1.build['foo'])


        self.assertIn('google', t1.sources.keys())
        self.assertIn('yahoo', t1.sources.keys())
        self.assertEquals('http://yahoo.com', t1.sources.yahoo.url)


        t2 = Top()

        t2.load_rows(t1.rows)

        self.assertEquals(self.yaml_config.strip(' \n'), t2.dump().strip(' \n'))

    def test_read_write(self):
        import yaml
        import tempfile
        from ambry.bundle.meta import Top
        import shutil, os

        d = tempfile.mkdtemp('metadata-test')

        t1 = Top(yaml.load(self.yaml_config))

        t1.write_to_dir(d)

        t2 = Top()
        t2.load_from_dir(d)

        self.assertEquals(self.yaml_config.strip(' \n'), t2.dump().strip(' \n'))

        #Test lazy loading.
        t3 = Top(path=d)
        self.assertTrue('license',t3.about.license)
        self.assertTrue('creator.email',t3.contact_bundle.creator.email)
        self.assertTrue(1,t3.nonterm.a)

        t3.write_to_dir(d)

        t4 = Top()
        t4.load_from_dir(d)

        self.assertEquals(self.yaml_config.strip(' \n'), t4.dump().strip(' \n'))

        t5 = Top(path=d)


        # Check that load from dir strips out erroneous terms
        # This depends on write_to_dur not checking and stripping these values.

        t5 = Top(path=d)
        t5.load_all()

        t5._term_values.about.foobar = 'foobar'
        t5._term_values.sources.foobar = 'foobar'
        t5._term_values.contact_bundle.creator.foobar = 'foobar'


        t5.write_to_dir(d)

        t52 = Top(path=d)
        t52.load_all()

        self.assertEquals(self.yaml_config.strip(' \n'), t52.dump().strip(' \n'))

        self.assertEquals(3,len(t52.errors))
        self.assertIn(('contact_bundle', 'creator', 'foobar'), t52.errors.keys())


        # Does it handle missing files?

        os.remove(os.path.join(d, 'meta/build.yaml'))

        t6 = Top()
        t6.load_from_dir(d)

        shutil.rmtree(d)


        p = '/data/source/clarinova-public/abc.ca.gov/alcohol_licenses'
        t7 = Top(path=d)
        t7.load_all()

        print t7.dump()


    def test_assignment(self):
        from ambry.bundle.meta import Top
        from ambry.identity import Identity
        import yaml

        t1 = Top(yaml.load(self.yaml_config))

        self.assertEquals(self.yaml_config.strip(' \n'), t1.dump().strip(' \n'))

        idnt = Identity.from_dict(dict(t1.identity))

        idd = idnt.ident_dict
        idd['variation'] = 'v2'

        t1.identity = idd

        self.assertEquals('v2', t1.identity.variation)



    def test_forced_format(self):
        yaml_config= """
external_documentation:
    foodoc1:
        description: description
        title: title1
        url: url1
    foodoc2:
        description: description2
    foodoc3:
        title: title3
"""
        from ambry.bundle.meta import Top
        from ambry.identity import Identity
        import yaml

        t1 = Top(yaml.load(yaml_config))


        print t1.dump(keys=['external_documentation'])

        yaml_config = """
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
    table: csv """

        t2 = Top(yaml.load(yaml_config))

        print t2.dump(keys=['partitions'])


    def test_links(self):
        from ambry.bundle.meta import Top
        import yaml

        t = Top(yaml.load(self.yaml_config))

        t.contact_bundle.creator.name = 'Bob Bobson'

        print t.contact_bundle.creator.name

        idd = dict(t.identity)

        t.identity = idd

        print t.dump()

    def test_errors(self):
        from ambry.bundle.meta import Top
        import yaml

        # Check that the invalid fields are removed.

        yaml_config = """
about:
    maintainer: maintainer
    homepage: homepage
    foo: bar
    license: license
    rights: rights
    subject: subject
    url: url"""

        t1 = Top(yaml.load(yaml_config))

        yc2 = yaml.load(t1.dump(keys=['about']))

        self.assertIn('rights', yc2['about'])
        self.assertNotIn('author', yc2['about'])
        self.assertNotIn('url', yc2['about'])
        self.assertIsNone(t1.about.summary)

        self.assertIn(('about', 'foo', None), t1.errors)

        t1 = Top(yaml.load(yaml_config), synonyms={'about.foo': 'about.summary'})

        self.assertEquals('bar',t1.about.summary)
        self.assertNotIn(('about', 'foo', None), t1.errors)

        print t1.errors

    def test_html(self):
        from ambry.bundle.meta import Top
        import yaml

        t = Top(yaml.load(self.yaml_config))
        print t.about.html()




def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())