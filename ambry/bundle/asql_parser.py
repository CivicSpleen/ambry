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
    Forward, restOfLine, Keyword, OneOrMore

# Public interface
#


def parse_view(query):
    return _view_stmt.parseString(query)


def parse_index(query):
    return _index_stmt.parseString(query)


# Parser implementation
#

def _flat_column(t):
    """ Populates column fields from parse result. """
    t.name = t.parsed_name[0]
    t.alias = t.parsed_alias[0] if t.parsed_alias else ''
    return t


# define SQL tokens
select_stmt = Forward()
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
    ident.setResultsName('parsed_name')
    + Optional(as_kw.suppress() + ident.setResultsName('parsed_alias'))
).setParseAction(_flat_column)

select_column_list = OneOrMore(Group(select_column + Optional(',').suppress()))

# define asql specific table name. May contain partition names: example.com-simple-simple1 for example
table_ident = ~reserved_words + Word(alphas, alphanums + '_-$').setName('table_identifier')
table_name = delimitedList(table_ident, '.', combine=True)
table_name_list = Group(delimitedList(table_name))

# define the select grammar
select_stmt << (
    select_kw
    + (Keyword('*').setResultsName('columns') | select_column_list.setResultsName('columns'))
    + from_kw
    + table_name_list.setResultsName('tables'))

# define create view grammar
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

# define index grammar
index_kw = Keyword('index', caseless=True)
_index_stmt = Forward()
_index_stmt << (
    index_kw
    + '(' + column_name_list.setResultsName('columns') + ')')

# define Oracle comment format, and ignore them
oracle_sql_comment = '--' + restOfLine
_view_stmt.ignore(oracle_sql_comment)
_index_stmt.ignore(oracle_sql_comment)
