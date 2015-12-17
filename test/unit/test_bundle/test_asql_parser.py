# -*- coding: utf-8 -*-

from ambry.bundle.asql_parser import parse_view, parse_index

from test.test_base import TestBase


class TestViewParser(TestBase):

    def test_create_view(self):
        query = 'CREATE VIEW view1 AS SELECT col1, col2 FROM example.com-simple-simple1;'
        view = parse_view(query)
        self.assertEqual(view.name, 'view1')
        self.assertEqual(view.tables[0].name, 'example.com-simple-simple1')
        self.assertEqual([x.name for x in view.columns], ['col1', 'col2'])

    def test_create_view_having_column_aliases(self):
        query = '''CREATE VIEW view1 AS SELECT col1 as c1, col2 as c2
            FROM example.com-simple-simple1;'''
        view = parse_view(query)
        self.assertEqual(view.name, 'view1')
        self.assertEqual(view.tables[0].name, 'example.com-simple-simple1')
        self.assertEqual(view.columns[0].name, 'col1')
        self.assertEqual(view.columns[0].alias, 'c1')
        self.assertEqual(view.columns[1].name, 'col2')
        self.assertEqual(view.columns[1].alias, 'c2')

    def test_create_view_having_table_alias(self):
        query = '''CREATE VIEW view1 AS SELECT t1.col1 AS t1_col1, t1.col2 AS t1_col2
            FROM example.com-simple-simple1 AS t1'''
        view = parse_view(query)
        self.assertEqual(view.name, 'view1')
        self.assertEqual(view.tables[0].name, 'example.com-simple-simple1')
        self.assertEqual(view.tables[0].alias, 't1')
        self.assertEqual(view.columns[0].name, 't1.col1')
        self.assertEqual(view.columns[0].alias, 't1_col1')
        self.assertEqual(view.columns[1].name, 't1.col2')
        self.assertEqual(view.columns[1].alias, 't1_col2')

    def test_create_view_having_join(self):
        query = '''CREATE VIEW view1 AS SELECT t1.col1 AS t1_col1, t1.col2 AS t1_col2
            FROM example.com-simple-simple1 AS t1
            LEFT JOIN example.com-simple-simple2 AS t2 ON t1.id = t2.id;
        '''
        view = parse_view(query)
        self.assertEqual(view.name, 'view1')
        self.assertEqual(view.tables[0].name, 'example.com-simple-simple1')
        self.assertEqual(view.columns[0].name, 't1.col1')
        self.assertEqual(view.columns[0].alias, 't1_col1')
        self.assertEqual(view.columns[1].name, 't1.col2')
        self.assertEqual(view.columns[1].alias, 't1_col2')

    def test_create_materialized_view(self):
        query = 'CREATE MATERIALIZED VIEW view1 AS SELECT col1, col2 FROM example.com-simple-simple1;'
        mat_view = parse_view(query)
        self.assertEqual(mat_view['name'], 'view1')
        self.assertEqual(mat_view['tables'][0].name, 'example.com-simple-simple1')
        self.assertEqual([x.name for x in mat_view.columns], ['col1', 'col2'])


class TestCreateIndexParser(TestBase):
    def test_create_index(self):
        index = parse_index('INDEX (col1, col2);')
        self.assertEqual(list(index['columns']), ['col1', 'col2'])
