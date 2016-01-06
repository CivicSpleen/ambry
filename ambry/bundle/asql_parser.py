"""
Parser for the *.asql files - ambry sql files (unified sql for postgres and sqlite)

Example:
    >>> from ambry.bundle.asql_parser import parse_view
    >>> view = parse_view('CREATE VIEW view1 AS SELECT col1 as c1, col2 as c2 FROM table1;')
    >>> print(view.name)
    view1
    >>> print(view.columns[0].name, view.columns[0].alias)
    col1, c1
    >>> print(view.columns[1].name, view.columns[1].alias)
    col2, c2
"""

from pyparsing import Word, delimitedList, Optional, Combine, Group, alphas, alphanums,\
    Forward, restOfLine, Keyword, OneOrMore, ZeroOrMore, Suppress

# Public interface
#


class Source(object):
    """ Parsed source - table name or partition ref. """

    def __init__(self, parsed_source):
        self.name = parsed_source.name
        self.alias = parsed_source.alias

    def __str__(self):
        return 'name: {}, alias: {}'.format(self.name, self.alias)


class Column(object):
    """ Parsed column. """

    def __init__(self, parsed_column):
        self.name = parsed_column.name
        self.alias = parsed_column.alias

    def __str__(self):
        return 'name: {}, alias: {}'.format(self.name, self.alias)


class Join(object):
    """ Parsed join. """

    def __init__(self, parsed_join):
        self.source = Source(parsed_join.source)

    def __str__(self):
        return self.source.__str__()


class View(object):
    """ Parsed view or materialized view. """

    def __init__(self, parse_result):
        self.name = parse_result.name
        self.sources = [Source(s) for s in parse_result.sources]
        self.columns = [Column(c) for c in parse_result.columns]
        self.joins = [Join(j) for j in parse_result.joins]

    def __str__(self):

        def wr(o):
            """ converts object to str and wraps it with curly braces. """
            return '{%s}' % o

        columns_str = ', '.join([wr(c) for c in self.columns])
        sources_str = ', '.join([wr(s) for s in self.sources])
        joins_str = ', '.join([wr(j) for j in self.joins])
        return 'name: {},\n sources: [{}],\n columns: [{}],\n joins: [{}]'.format(
            self.name, sources_str, columns_str, joins_str)


class Index(object):
    """ Parsed index. """

    def __init__(self, parse_result):
        self.source = parse_result.source
        self.columns = list(parse_result.columns)


def parse_view(query):
    """ Parses asql query to view object.

    Args:
        query (str): asql query

    Returns:
        View instance: parsed view.
    """
    return View(_view_stmt.parseString(query))


def parse_index(query):
    """ Parses asql query to view object.

    Args:
        query (str): asql index create query.
            Example: 'INDEX example.com-simple-simple (id, uuid);'

    Returns:
        Index instance: parsed index.
    """
    return Index(_index_stmt.parseString(query))


# Parser implementation
#

def _flat_alias(t):
    """ Populates token (column or table) fields from parse result. """
    t.name = t.parsed_name
    t.alias = t.parsed_alias[0] if t.parsed_alias else ''
    return t


def _build_join(t):
    """ Populates join token fields. """
    t.source.name = t.source.parsed_name
    t.source.alias = t.source.parsed_alias[0] if t.source.parsed_alias else ''
    return t


# define SQL tokens
comma_token = Suppress(',')
select_kw = Keyword('select', caseless=True)
update_kw = Keyword('update', caseless=True)
volatile_kw = Keyword('volatile', caseless=True)
create_kw = Keyword('create', caseless=True)
table_kw = Keyword('table', caseless=True)
as_kw = Keyword('as', caseless=True)
from_kw = Keyword('from', caseless=True)
where_kw = Keyword('where', caseless=True)
join_kw = Keyword('join', caseless=True)
left_kw = Keyword('left', caseless=True)
right_kw = Keyword('right', caseless=True)
cross_kw = Keyword('cross', caseless=True)
outer_kw = Keyword('outer', caseless=True)
inner_kw = Keyword('inner', caseless=True)
natural_kw = Keyword('natural', caseless=True)
on_kw = Keyword('on', caseless=True)
insert_kw = Keyword('insert', caseless=True)
into_kw = Keyword('into', caseless=True)

# define sql reserved words
reserved_words = (
    update_kw | volatile_kw | create_kw | table_kw
    | as_kw | from_kw | where_kw | join_kw | left_kw
    | right_kw | cross_kw | outer_kw | on_kw | insert_kw | into_kw
)

ident = ~reserved_words + Word(alphas, alphanums + '_$').setName('identifier')

column_name = delimitedList(ident, '.', combine=True)
column_name_list = Group(delimitedList(column_name))

# column from the select statement with `as` keyword support.
select_column = (
    delimitedList(ident, '.', combine=True).setResultsName('parsed_name')
    + Optional(as_kw.suppress() + delimitedList(ident, '.', combine=True)).setResultsName('parsed_alias')
).setParseAction(_flat_alias)
# Examples:
# column = select_column.parseString('col1 as c1')
# print(column.name, column.alias)
# (col1, c1)

select_column_list = OneOrMore(Group(select_column + Optional(comma_token)))
# Examples:
# columns = select_column_list.parseString('col1 as c1, col2 as c2')
# print(columns[0].name, columns[0].alias)
# (col1, c1)
# print(columns[1].name, columns[1].alias)
# (col2, c2)

# define asql specific table name. May contain partition names: example.com-simple-simple1 for example
#
source_ident = ~reserved_words + Word(alphas, alphanums + '_-$').setName('source_identifier')
source = (
    delimitedList(source_ident, '.', combine=True).setResultsName('parsed_name')
    + Optional(as_kw.suppress() + ident).setResultsName('parsed_alias')
).setParseAction(_flat_alias)
# Example:
# parsed = source.parseString('table1 as t1')
# print(parsed.name, parsed.alias)
# table1, t1

many_sources = OneOrMore(Group(source + Optional(comma_token)))
# Example:
# parsed = many_sources.parseString('table1 as t1, table2 as t2')
# print(parsed[0].name, parsed[0].alias)
# table1, t1
# print(parsed[1].name, parsed[1].alias)
# table2, t2


# define join grammar
#
join_op = (
    comma_token
    | (
        Optional(natural_kw)
        + Optional(inner_kw | cross_kw | left_kw + outer_kw | left_kw | outer_kw)
        + join_kw))


join_stmt = (join_op + source.setResultsName('source')).setParseAction(_build_join)
# Example
# join = join_stmt.parseString('join jtable1 as jt1')
# print(join.source.name, join.source.alias)
# (jtable1, jt1)

# define the select grammar
#
select_stmt = Forward()
select_stmt << (
    select_kw
    + (Keyword('*').setResultsName('columns') | select_column_list.setResultsName('columns'))
    + from_kw
    + many_sources.setResultsName('sources')
    + ZeroOrMore(Group(join_stmt)).setResultsName('joins'))
# Examples:
# select = select_stmt.parseString(
#     '''SELECT t1.col AS t1_c, t2.col AS t2_c, t3.col AS t3_c
#     FROM table1 AS t1
#     JOIN table2 AS t2
#     JOIN table3 AS t3;''')
# print(select.columns[0].name, select.columns[0].alias)
# ('t1.col', 't1_c')
# print(select.columns[1].name, select.columns[1].alias)
# ('t2.col', 't2_c')
# print(select.columns[2].name, select.columns[2].alias)
# ('t3.col', 't2_c')
# print(select.sources[0].name, select.sources[0].alias)
# ('table1', 't1')
# print(select.joins[0].source.name, select.joins[0].source.alias)
# ('table2', 't2')
# print(select.joins[1].source.name, select.joins[1].source.alias)
# ('table3', 't3')

#
# Define create view grammar.
#
materialized_kw = Keyword('materialized', caseless=True)
view_token = Keyword('view', caseless=True)
_view_stmt = Forward()
_view_stmt << (
    create_kw
    + Optional(materialized_kw)
    + view_token
    + Combine(ident).setResultsName('name')
    + as_kw
    + select_stmt
)

#
# Define asql index grammar.
#
index_source = delimitedList(source_ident, '.', combine=True)
index_kw = Keyword('index', caseless=True)
_index_stmt = Forward()
_index_stmt << (
    index_kw
    + index_source.setResultsName('source')
    + '(' + column_name_list.setResultsName('columns') + ')')
# Examples:
# index = index_stmt.parseString('INDEX partition1 (col1, col2, col3);')
# print(index.source)
# 'partition1'
# print(index.columns)
# ['col1', 'col2', 'col3']

# define Oracle comment format, and ignore them
oracle_sql_comment = '--' + restOfLine
_view_stmt.ignore(oracle_sql_comment)
_index_stmt.ignore(oracle_sql_comment)
