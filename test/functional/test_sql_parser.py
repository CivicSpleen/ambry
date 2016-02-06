

from test.proto import TestBase

class Test(TestBase):


    def test_parser_basic(self):

        from ambry.bundle.asql_parser import parse_view, parse_select

        view = parse_view('CREATE VIEW view1 AS SELECT col1 as c1, col2 as c2 FROM table1;')

        self.assertEquals('view1',view.name)

        self.assertEquals('col1',view.columns[0].name)
        self.assertEquals('c1',view.columns[0].alias)
        self.assertEquals('col2',view.columns[1].name)
        self.assertEquals('c2',view.columns[1].alias)

        select = parse_select('SELECT col1 as c1, col2 as c2 FROM table1;')

        self.assertEquals('col1', select.columns[0].name)
        self.assertEquals('c1', select.columns[0].alias)
        self.assertEquals('col2', select.columns[1].name)
        self.assertEquals('c2', select.columns[1].alias)

        select = parse_select(
            '''SELECT t1.col AS t1_c, t2.col AS t2_c, t3.col AS t3_c
            FROM table1 AS t1
            JOIN table2 AS t2
            JOIN table3 AS t3;''')

        self.assertEquals('t1.col',select.columns[0].name)
        self.assertEquals('t1_c',select.columns[0].alias)

        self.assertEquals('t2.col', select.columns[1].name)
        self.assertEquals('t2_c', select.columns[1].alias)

        self.assertEquals('t3.col', select.columns[2].name)
        self.assertEquals('t3_c',select.columns[2].alias)

        self.assertEquals('table1',select.sources[0].name)
        self.assertEquals('t1', select.sources[0].alias)

        self.assertEquals('table2',select.joins[0].source.name)
        self.assertEquals('t2', select.joins[0].source.alias)

        self.assertEquals('table3',select.joins[1].source.name)
        self.assertEquals('t3', select.joins[1].source.alias)

        select = parse_select(
            '''SELECT t1.col AS t1_c, t2.col AS t2_c, t3.col AS t3_c
            FROM cdph.ca.gov-hci-high_school_ed-county AS t1
            JOIN cdph.ca.gov-hci-high_school_ed-city AS t2 ON t1_c = t2_c
            JOIN cdph.ca.gov-hci-high_school_ed-state AS t3 ON t1_c = t3_c;
            ''')

        self.assertEquals('t1.col', select.columns[0].name)
        self.assertEquals('t1_c', select.columns[0].alias)

        self.assertEquals('t2.col', select.columns[1].name)
        self.assertEquals('t2_c', select.columns[1].alias)

        self.assertEquals('t3.col', select.columns[2].name)
        self.assertEquals('t3_c', select.columns[2].alias)

        self.assertEquals('cdph.ca.gov-hci-high_school_ed-county', select.sources[0].name)
        self.assertEquals('t1', select.sources[0].alias)

        self.assertEquals('cdph.ca.gov-hci-high_school_ed-city', select.joins[0].source.name)
        self.assertEquals('t2', select.joins[0].source.alias)

        self.assertEquals('cdph.ca.gov-hci-high_school_ed-state', select.joins[1].source.name)
        self.assertEquals('t3', select.joins[1].source.alias)

    def test_identifier_replacement(self):
        from ambry.bundle.asql_parser import substitute_vids

        l = self.library()

        self.assertEquals('SELECT * FROM p00casters006003',
                          substitute_vids(l, 'SELECT * FROM build.example.com-casters-simple')[0])

        self.assertEquals('SELECT * FROM p00casters006003 LEFT JOIN pERJQxWUVb005001 ON foo = bar',
                          substitute_vids(l,
                                          """SELECT * FROM build.example.com-casters-simple
                                             LEFT JOIN build.example.com-generators-demo ON foo = bar
                                          """)[0])

