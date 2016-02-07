

from test.proto import TestBase

class Test(TestBase):


    def test_parser_basic(self):

        from ambry.bundle.asql_parser import parse_view, parse_select

        view = parse_view('CREATE VIEW view1 AS SELECT col1 as c1, col2 as c2 FROM table1 WHERE foo is None and bar is baz;')

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
        self.assertEquals(['t1_c', 't2_c'], list(select.joins[0].join_cols))

        self.assertEquals('cdph.ca.gov-hci-high_school_ed-state', select.joins[1].source.name)
        self.assertEquals('t3', select.joins[1].source.alias)
        self.assertEquals(['t1_c', 't3_c'], select.joins[1].join_cols)

        select = parse_view(
            '''CREATE VIEW foobar AS SELECT t1.col AS t1_c, t2.col AS t2_c, t3.col AS t3_c
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
        self.assertEquals(['t1_c', 't2_c'], list(select.joins[0].join_cols))

        self.assertEquals('cdph.ca.gov-hci-high_school_ed-state', select.joins[1].source.name)
        self.assertEquals('t3', select.joins[1].source.alias)
        self.assertEquals(['t1_c', 't3_c'], select.joins[1].join_cols)

        stmt = """
            SELECT t1.uuid AS t1_uuid, t2.float_a AS t2_float_a, t3.a AS t3_a
                FROM build.example.com-casters-simple AS t1
                JOIN build.example.com-casters-simple_stats AS t2 ON t1.id = t2.index
                JOIN build.example.com-casters-integers AS t3 ON t3_a = t2.index
            WHERE foo = bar
            """

        select = parse_select(stmt)

        self.assertEqual(2, len(select.joins))

    def test_visitor(self):
        import sqlparse
        from ambry.bundle.asql_parser import find_indexable_materializable

        stmt = """
        SELECT t1.uuid AS t1_uuid, t2.float_a AS t2_float_a, t3.a AS t3_a
            FROM build.example.com-casters-simple AS t1
            JOIN build.example.com-casters-simple_stats AS t2 ON t1.id = t2.index
            JOIN build.example.com-casters-integers AS t3 ON t3_a = t2.index
        """

        library = self.library()

        stmt, drop, tables, install, materialize, indexes = find_indexable_materializable(stmt, library)

        self.assertEquals(sorted([u'p00casters002003', u'p00casters004003', u'p00casters006003']),
                          sorted(materialize))
        self.assertEquals(sorted([(u'p00casters006003', [u'id']),
                           (u'p00casters002003', [u'index']),
                           (u'p00casters002003', [u'index'])]),
                           sorted(indexes))

        sql = """

INSTALL build.example.com-casters-simple;
INSTALL build.example.com-casters-simple_stats;
MATERIALIZE build.example.com-casters-integers;
MATERIALIZE build.example.com-casters-simple_stats;

SELECT t1.uuid AS t1_uuid, t2.float_a AS t2_float_a, t3.a AS t3_a
    FROM build.example.com-casters-simple AS t1
    JOIN build.example.com-casters-simple_stats AS t2 ON t1.id = t2.index
    JOIN build.example.com-casters-integers AS t3 ON t3_a = t2.index;


CREATE VIEW view1 AS SELECT col1 as c1, col2 as c2 FROM table1 WHERE foo is None and bar is baz;

"""

        statements = sqlparse.parse(sqlparse.format(sql, strip_comments=True))


        expected_statements = [
            'INSTALL p00casters006003',
            'INSTALL p00casters002003',
            'MATERIALIZE p00casters004003',
            'MATERIALIZE p00casters002003',
            'SELECT t1.uuid AS t1_uuid, t2.float_a AS t2_float_a, t3.a AS t3_a FROM '
            'p00casters006003 AS t1 JOIN p00casters002003 AS t2 ON t1.id = t2.index '
            'JOIN p00casters004003 AS t3 ON t3_a = t2.index',
            'CREATE VIEW view1 AS SELECT col1 as c1, col2 as c2 FROM table1 WHERE foo is None and bar is baz'

        ]

        expected_parts = [
            [[], [u'p00casters006003'], [], []],
            [[], [u'p00casters002003'], [], []],
            [[], [], [u'p00casters004003'], []],
            [[], [], [u'p00casters002003'], []],
            [[], [], [u'p00casters004003', u'p00casters006003', u'p00casters002003'],
             [(u'p00casters006003', [u'id']), (u'p00casters002003',
                                               [u'index']), (u'p00casters002003', [u'index'])]],
            [[], [], [], []]
        ]

        for i, stmt in enumerate(statements):

            actual_statement, drop, tables, install, materialize, indexes = find_indexable_materializable(stmt, library)

            if not actual_statement:
                continue

            parts = [tables, install, materialize, indexes]
            self.assertEqual(expected_statements[i], str(actual_statement))

            for actual_part, expected_part in zip(parts, expected_parts[i]):
                self.assertEqual(expected_part, actual_part)

        sql = """


MATERIALIZE build.example.com-casters-integers;
MATERIALIZE build.example.com-casters-simple_stats;
INSTALL build.example.com-casters-simple;

CREATE VIEW simple_stats AS
SELECT * FROM build.example.com-casters-simple_stats;

SELECT * FROM simple_stats AS ss
JOIN build.example.com-casters-integers as intr ON intr.a = ss.id;

SELECT * FROM build.example.com-casters-simple;


"""

        w = library.warehouse()

        rows = list(w.query(sql))

        self.assertTrue('Alabama' in rows[0])
        self.assertTrue('Alaska' in rows[1])

        # As before, but without the installs.
        sql = """

    CREATE VIEW simple_stats AS
    SELECT * FROM build.example.com-casters-simple_stats;

    SELECT * FROM simple_stats AS ss
    JOIN build.example.com-casters-integers as intr ON intr.a = ss.id;

    SELECT * FROM build.example.com-casters-simple;


    """

        w = library.warehouse()


        rows = list(w.query(sql))

        self.assertTrue('Alabama' in rows[0])
        self.assertTrue('Alaska' in rows[1])


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

