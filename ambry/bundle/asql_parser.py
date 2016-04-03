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
    Forward, restOfLine, Keyword, OneOrMore, ZeroOrMore, Suppress, quotedString

# Public interface
#


def parse_select(query):
    """ Parses asql query to view object.

    Args:
        query (str): asql query

    Returns:
        View instance: parsed view.
    """
    result = select_stmt.parseString(query)

    return Select(result)


def parse_view(query):
    """ Parses asql query to view object.

    Args:
        query (str): asql query

    Returns:
        View instance: parsed view.
    """

    try:
        idx = query.lower().index('where')
        query = query[:idx]
    except ValueError:
        pass

    if not query.endswith(';'):
        query = query.strip()
        query += ';'

    result = _view_stmt.parseString(query)

    return View(result)


def parse_index(query):
    """ Parses asql query to view object.

    Args:
        query (str): asql index create query.
            Example: 'INDEX example.com-simple-simple (id, uuid);'

    Returns:
        Index instance: parsed index.
    """
    return Index(_index_stmt.parseString(query))


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

        try:
            self.name = parsed_column.name
            self.alias = parsed_column.alias
        except AttributeError:
            # Assume the value is a string, usualla single column name or '*'
            self.name = parsed_column
            self.alias = None

    def __str__(self):
        return 'name: {}, alias: {}'.format(self.name, self.alias)


class Join(object):
    """ Parsed join. """

    def __init__(self, parsed_join):
        self.source = Source(parsed_join.source)
        self.join_cols = parsed_join.join_cols.asList() if parsed_join.join_cols else None

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


class Select(object):
    """ Parsed select """

    def __init__(self, parse_result):

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
on_kw = Keyword('on', caseless=True)
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
    | as_kw | from_kw | where_kw | join_kw | on_kw | left_kw
    | right_kw | cross_kw | outer_kw | on_kw | insert_kw | into_kw
)

ident = ~reserved_words + Word(alphas, alphanums + '_$.').setName('identifier')

function = (
    (Word(alphas) + '(' + Word(alphanums + '*_$.') + ')')
    + Optional(as_kw.suppress() + delimitedList(ident, '.', combine=True))
)

column_name = delimitedList(ident, '.', combine=True)
column_name_list = Group(delimitedList(column_name))

# To make it simple, enclode all expressions in parens.
expression = "(" + OneOrMore( Word(alphanums + '_-.') | Word('-+*')) + ")"

# column from the select statement with `as` keyword support.
# the quoted string handles cases like : SELECT 'CA' AS state
select_column = (
    ( delimitedList(ident, '.', combine=True).setResultsName('parsed_name') | quotedString | expression )
    + Optional(as_kw.suppress() + delimitedList(ident, '.', combine=True)).setResultsName('parsed_alias')
).setParseAction(_flat_alias)

# Examples:
# column = select_column.parseString('col1 as c1')
# print(column.name, column.alias)
# (col1, c1)

select_column_list = OneOrMore(Group((function | select_column ) + Optional(comma_token)))

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

on_op = Optional(on_kw.suppress() + ident + Word('=').suppress() + ident)

join_op = (
    comma_token
    | (Optional(natural_kw)
       + Optional(inner_kw | cross_kw | left_kw + outer_kw | left_kw | outer_kw)
       + join_kw)
)

join_stmt = (
    join_op + source.setResultsName('source') + on_op.setResultsName('join_cols')).setParseAction(_build_join)

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
table_token = Keyword('table', caseless=True) # Now, it is is also a table statement, not just a view statement
_view_stmt = Forward()
_view_stmt << (
    create_kw
    + Optional(materialized_kw)
    + (view_token | table_token)
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
    Optional(create_kw)
    + index_kw
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


def substitute_vids(library, statement):
    """ Replace all of the references to tables and partitions with their vids.

    This is a bit of a hack -- it ought to work with the parser, but instead it just looks for
    common SQL tokens that indicate an identifier.

    :param statement: an sqlstatement. String.
    :return: tuple: new_statement, set of table vids, set of partition vids.
    """
    from ambry.identity import ObjectNumber, TableNumber, NotObjectNumberError
    from ambry.orm.exc import NotFoundError

    try:
        stmt_str = statement.to_unicode()
    except AttributeError:
        stmt_str = statement

    parts = stmt_str.strip(';').split()

    new_parts = []

    tables = set()
    partitions = set()

    while parts:
        token = parts.pop(0).strip()
        if token.lower() in ('from', 'join', 'materialize', 'install'):
            ident = parts.pop(0).strip(';')
            new_parts.append(token)

            try:
                obj_number = ObjectNumber.parse(token)
                if isinstance(obj_number, TableNumber):
                    table = library.table(ident)
                    tables.add(table.vid)
                    new_parts.append(table.vid)
                else:
                    # Do not care about other object numbers. Assume partition.
                    raise NotObjectNumberError

            except NotObjectNumberError:
                # assume partition
                try:
                    partition = library.partition(ident)
                    partitions.add(partition.vid)
                    new_parts.append(partition.vid)
                except NotFoundError:
                    # Ok, maybe it is just a normal identifier...
                    new_parts.append(ident)
        else:
            new_parts.append(token)

    return ' '.join(new_parts).strip(), tables, partitions


def validate(sql):
    """
    Parse a SQL statement and enforce some rules.

    The rules are:

    * If there are JOINs, all tables must be aliased.
    * In the join clauses, all columns must have dotted forms, using the table aliases.

    :param sql:
    :return:
    :raises: Exception that includes a list of the errors.
    """

    pass


class FIMRecord(object):

    def __init__(self, statement, drop=None, tables=None, install=None,
                 materialize=None, indexes=None, joins=0, views=0):

        self.statements = None
        self.statement = statement
        self.drop = [drop] if drop else []
        self.tables = set(tables) if tables else set()
        self.install = set(install) if install else set()
        self.materialize = set(materialize) if materialize else set()
        self.indexes = set(indexes) if indexes else set()
        self.joins = joins
        self.views = views

    def update(self, rec=None, drop=None, tables=None, install=None, materialize=None,
               indexes=None, joins=0, views=0):
        """ Updates current record.

        Args:
            rec (FIMRecord):
        """
        if not drop:
            drop = []

        if not tables:
            tables = set()

        if not install:
            install = set()

        if not materialize:
            materialize = set()

        if not indexes:
            indexes = set()

        if rec:
            self.update(
                drop=rec.drop, tables=rec.tables, install=rec.install, materialize=rec.materialize,
                indexes=rec.indexes, joins=rec.joins
            )

        self.drop += drop
        self.tables |= set(tables)
        self.install |= set(install)
        self.materialize |= set(materialize)
        self.indexes |= set(indexes)

        self.joins += joins
        self.views += views

        # Joins or views promote installed partitions to materialized partitions
        if self.joins > 0 or self.views > 0:
            self.materialize |= self.install
            self.install = set()


def find_indexable_materializable(sql, library):
    """
    Parse a statement, then call functions to install, materialize or create indexes for partitions
    referenced in the statement.

    :param sql:
    :param materialize_f:
    :param install_f:
    :param index_f:
    :return:
    """

    derefed, tables, partitions = substitute_vids(library, sql)

    if derefed.lower().startswith('create index') or derefed.lower().startswith('index'):
        parsed = parse_index(derefed)
        return FIMRecord(statement=derefed, indexes=[(parsed.source, tuple(parsed.columns))])

    elif derefed.lower().startswith('materialize'):
        _, vid = derefed.split()
        return FIMRecord(statement=derefed, materialize=set([vid]))

    elif derefed.lower().startswith('install'):
        _, vid = derefed.split()
        return FIMRecord(statement=derefed, install=set([vid]))

    elif derefed.lower().startswith('select'):
        rec = FIMRecord(statement=derefed)
        parsed = parse_select(derefed)

    elif derefed.lower().startswith('drop'):
        return FIMRecord(statement=derefed, drop=derefed)

    elif derefed.lower().startswith('create table'):
        parsed = parse_view(derefed)
        rec = FIMRecord(statement=derefed, drop='DROP TABLE IF EXISTS {};'.format(parsed.name), views=1)

    elif derefed.lower().startswith('create view'):
        parsed = parse_view(derefed)
        rec = FIMRecord(statement=derefed, drop='DROP VIEW IF EXISTS {};'.format(parsed.name), views=1)
    else:
        return FIMRecord(statement=derefed, tables=set(tables), install=set(partitions))

    def partition_aliases(parsed):
        d = {}

        for source in parsed.sources:
            if source.alias:
                d[source.alias] = source.name

        for j in parsed.joins:
            if j.source.alias:
                d[j.source.alias] = j.source.name

        return d

    def indexable_columns(aliases, parsed):

        indexes = []

        for j in parsed.joins:
            if j and j.join_cols:
                for col in j.join_cols:
                    if '.' in col:
                        try:
                            alias, col = col.split('.')
                            if alias:
                                indexes.append((aliases[alias], (col,)))
                        except KeyError:
                            pass

        return indexes

    aliases = partition_aliases(parsed)

    indexes = indexable_columns(aliases, parsed)

    rec.joins = len(parsed.joins)

    install = set(partitions)

    rec.update(tables=tables, install=install, indexes=indexes)

    return rec


def process_sql(sql, library):
    import sqlparse

    processed_statements = []
    statements = sqlparse.parse(sqlparse.format(sql, strip_comments=True))

    sum_rec = FIMRecord(None)
    for parsed_statement in statements:
        rec = find_indexable_materializable(parsed_statement, library)

        sum_rec.update(rec=rec)

        processed_statements.append(rec.statement)

    sum_rec.statements = processed_statements

    return sum_rec
