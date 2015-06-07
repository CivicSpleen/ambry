"""
Created on Aug 31, 2012

@author: eric
"""
import unittest

from test_base import TestBase


class Test(TestBase):
    def setUp(self):
        super(Test, self).setUp()

        self.yaml_config = """
about:
    access: null
    footnote: null
    grain: null
    groups:
    - Group 1
    - Group 2
    license: license
    processed: null
    rights: rights
    source: null
    space: null
    subject: subject
    summary: summary
    tags:
    - Tag1
    - Tag 2
    time: null
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
coverage:
    geo: []
    grain: []
    time: []
dependencies: {}
documentation:
    footnote: null
    main: null
    processed: null
    readme: null
    source: null
    summary: null
    title: null
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
        date: null
        description: d3
        version: s3
views: {}
            """

    def tearDown(self):
        pass

    def test_dict_group(self):


        from ambry.metadata.meta import StructuredPropertyTree, ScalarTerm, DictGroup, DictTerm, ListTerm

        class TestDictTerm1(DictTerm):
            sterm = ScalarTerm()
            lterm = ListTerm()

        class TestDictTerm2(DictTerm):
            sterm = ScalarTerm()
            lterm = ListTerm()
            dterm = TestDictTerm1()

        class TestDictGroup(DictGroup):
            dterm1 = TestDictTerm2()

        class TestTop(StructuredPropertyTree):
            dictgroup = TestDictGroup()

        t = TestTop()

        t.dictgroup.dterm1.sterm = 'sterm'
        t.dictgroup.dterm1.lterm = [1,2,3]
        t.dictgroup.dterm1.dterm.sterm = 'sterm'
        t.dictgroup.dterm1.dterm.lterm = [1, 2, 3]

        self.assertEquals(t.dictgroup.dterm1.sterm, 'sterm')
        self.assertEquals(list(t.dictgroup.dterm1.lterm), [1,2,3])
        self.assertEquals(t.dictgroup.dterm1.dterm.sterm , 'sterm')
        self.assertEquals(list(t.dictgroup.dterm1.dterm.lterm) , [1, 2, 3])

        self.assertEquals(t.dictgroup.dterm1.dict,
                          {'lterm': [1, 2, 3], 'sterm': 'sterm', 'dterm': {'sterm': 'sterm', 'lterm': [1, 2, 3]}})

        self.assertEquals(t.dictgroup.dterm1.dterm.dict,{'sterm': 'sterm', 'lterm': [1, 2, 3]})



    def x_test_basic(self):
        from ambry.metadata.meta import StructuredPropertyTree, ScalarTerm, TypedDictGroup, \
            VarDictGroup, DictGroup, DictTerm
        from ambry.util import AttrDict

        class TestDictTerm(DictTerm):
            dterm1 = ScalarTerm()
            dterm2 = ScalarTerm()
            unset_term = ScalarTerm()



        class TestGroup(DictGroup):
            term = ScalarTerm()
            term2 = ScalarTerm()
            dterm = TestDictTerm()

        class TestTDGroup(TypedDictGroup):
            _proto = TestDictTerm()

        class TestTop(StructuredPropertyTree):
            group = TestGroup()
            tdgroup = TestTDGroup()
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
        self.assertEquals(
            ['Term', 'Term2', AttrDict([('dterm1', ''), ('unset_term', ''), ('dterm2', '')])],
            tt.group.values())

        #
        # Dict Term

        tt.group.dterm.dterm1 = 'dterm1'
        tt.group.dterm.dterm2 = 'dterm2'

        self.assertEquals('dterm1', tt.group.dterm.dterm1)
        self.assertEquals(['dterm1', 'unset_term', 'dterm2'], tt.group.dterm.keys())
        self.assertEquals(['dterm1', '', 'dterm2'], tt.group.dterm.values())

        tt.group = dict(term='x',term2='y')
        self.assertEquals('x',tt.group.term)
        self.assertEquals('y',tt.group.term2)

        #
        # TypedDictGroup

        tt.tdgroup.foo.dterm1 = 'foo.dterm1'

        self.assertEqual('foo.dterm1', tt.tdgroup.foo.dterm1)

        tt.tdgroup.foo.dterm2 = 'foo.dterm2'
        tt.tdgroup.baz.dterm1 = 'foo.dterm1'

        #
        # VarDict Group

        tt.vdgroup.k1['v1'] = 'v1'
        tt.vdgroup.k1.v2 = 'v2'





def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
