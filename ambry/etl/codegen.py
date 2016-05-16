""" Code generation for processing columns

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import ast
import meta


def file_loc():
    """Return file and line number"""
    import sys
    import inspect
    try:
        raise Exception
    except:
        file_ = '.../' + '/'.join((inspect.currentframe().f_code.co_filename.split('/'))[-3:])
        line_ = sys.exc_info()[2].tb_frame.f_back.f_lineno
        return "{}:{}".format(file_, line_)


const_args = ('row', 'row_n', 'scratch', 'errors', 'accumulator', 'pipe', 'bundle', 'source')
var_args = ('v', 'i_s', 'i_d', 'header_s', 'header_d')
all_args = var_args + const_args

# Full lambda definition for a column, including variable parts
col_code_def = 'lambda {}:'.format(','.join(all_args))

# lambda definition for the who;e row. Includes only the arguments
# that are the same for every column
code_def = 'lambda {}:'.format(','.join(const_args))

col_args_t="""col_args = dict(v=v, i_s=i_s, i_d=i_d, header_s=header_s, header_d=header_d,
              scratch=scratch, errors=errors, accumulator = accumulator,
              row=row, row_n=row_n)"""

file_header = """

from ambry.valuetype import resolve_value_type

"""

column_template = """
def {f_name}(v, i_s, i_d, header_s, header_d, row, row_n, errors, scratch, accumulator, pipe, bundle, source):

    {col_args}

    try:
{stack}

    except Exception as exc:
{exception}

    return v
"""

indent = '        '

row_template = """
def row_{table}_{stage}(row, row_n, errors, scratch, accumulator, pipe, bundle, source):

    return [
{stack}
    ]
"""


class CodeGenError(Exception):
    pass


def make_env(bundle, base_env):
    def _ff(code):
        try:
            return base_env.get(code, None)
        except (AttributeError, KeyError):
            return None

    return _ff


def make_row_processors(bundle, source_headers, dest_table, env):
    """
    Make multiple row processors for all of the columns in a table.

    :param source_headers:

    :return:
    """

    dest_headers = [c.name for c in dest_table.columns]

    row_processors = []

    out = [file_header]

    transforms = list(dest_table.transforms)
    column_names = []
    column_types = []
    for i, segments in enumerate(transforms):

        seg_funcs = []

        for col_num, (segment, column) in enumerate(zip(segments, dest_table.columns), 1):

            if not segment:
                seg_funcs.append('row[{}]'.format(col_num - 1))
                continue

            assert column
            assert column.name == segment['column'].name
            col_name = column.name
            preamble, try_lines, exception = make_stack(env, i, segment)

            assert col_num == column.sequence_id, (dest_table.name, col_num, column.sequence_id)

            column_names.append(col_name)
            column_types.append(column.datatype)

            f_name = "{table_name}_{column_name}_{stage}".format(
                table_name=dest_table.name,
                column_name=col_name,
                stage=i
            )

            exception = (exception if exception else
                                  ('raise ValueError("Failed to cast column \'{}\', in '
                                   'function {}, value \'{}\': {}".format(header_d,"') + f_name + '", v, exc) ) ')

            try:
                i_s = source_headers.index(column.name)
                header_s = column.name
                v = 'row[{}]'.format(i_s)

            except ValueError as e:

                i_s = 'None'
                header_s = None
                v = 'None' if col_num > 1 else 'row_n' # Give the id column the row number


            i_d = column.sequence_id - 1

            header_d = column.name

            template_args = dict(
                f_name=f_name,
                table_name=dest_table.name,
                column_name=col_name,
                stage=i,
                i_s=i_s,
                i_d=i_d,
                header_s=header_s,
                header_d=header_d,
                v=v,
                exception=indent + exception,
                stack='\n'.join(indent + l for l in try_lines),
                col_args = '# col_args not implemented yet'
            )

            seg_funcs.append(f_name
                             + ('({v}, {i_s}, {i_d}, {header_s}, \'{header_d}\', '
                                'row, row_n, errors, scratch, accumulator, pipe, bundle, source)')
                             .format(v=v, i_s=i_s, i_d=i_d, header_s="'" + header_s + "'" if header_s else 'None',
                                     header_d=header_d))

            out.append('\n'.join(preamble))

            out.append(column_template.format(**template_args))

        source_headers = dest_headers

        stack = '\n'.join("{}{}, # {}".format(indent,l,cn)
                          for l,cn, dt in zip(seg_funcs, column_names, column_types))

        out.append(row_template.format(
            table=dest_table.name,
            stage=i,
            stack=stack
        ))

        row_processors.append('row_{table}_{stage}'.format(stage=i, table=dest_table.name))

    # Add the final datatype cast, which is done seperately to avoid an unecessary function call.

    stack = '\n'.join("{}cast_{}(row[{}], '{}', errors),".format(indent, c.datatype, i, c.name)
                      for i, c in enumerate(dest_table.columns) )

    out.append(row_template.format(
        table=dest_table.name,
        stage=len(transforms),
        stack=stack
    ))

    row_processors.append('row_{table}_{stage}'.format(stage=len(transforms), table=dest_table.name))

    out.append('row_processors = [{}]'.format(','.join(row_processors)))

    return '\n'.join(out)

def calling_code(f, f_name=None, raise_for_missing=True):
    """Return the code string for calling a function. """
    import inspect
    from ambry.dbexceptions import ConfigurationError

    if inspect.isclass(f):
        try:
            args = inspect.getargspec(f.__init__).args
        except TypeError as e:
            raise TypeError("Failed to inspect {}: {}".format(f, e))

    else:
        args = inspect.getargspec(f).args

    if len(args) > 1 and args[0] == 'self':
        args = args[1:]

    for a in args:
        if a not in all_args + ('exception',):  # exception arg is only for exception handlers
            if raise_for_missing:
                raise ConfigurationError('Caster code {} has unknown argument '
                                         'name: \'{}\'. Must be one of: {} '.format(f, a, ','.join(all_args)))

    arg_map = {e: e for e in var_args}

    args = [arg_map.get(a, a) for a in args]

    return "{}({})".format(f_name if f_name else f.__name__, ','.join(args))


def make_stack(env, stage, segment):
    """For each transform segment, create the code in the try/except block with the
    assignements for pipes in the segment """

    import string
    import random
    from ambry.valuetype import ValueType

    column = segment['column']

    def make_line(column, t):
        preamble = []

        line_t = "v = {} # {}"

        if isinstance(t, type) and issubclass(t, ValueType):  # A valuetype class, from the datatype column.

            try:
                cc, fl = calling_code(t, t.__name__), file_loc()
            except TypeError:
                cc, fl = "{}(v)".format(t.__name__), file_loc()

            preamble.append("{} = resolve_value_type('{}') # {}".format(t.__name__, t.vt_code, fl))

        elif isinstance(t, type):  # A python type, from the datatype columns.
            cc, fl= "parse_{}(v, header_d)".format(t.__name__), file_loc()

        elif callable(env.get(t)):  # Transform function
            cc, fl = calling_code(env.get(t), t), file_loc()

        else:  # A transform generator, or python code.

            assert 'cst_double' in env

            rnd = (''.join(random.choice(string.ascii_lowercase) for _ in range(6)))

            name = 'tg_{}_{}_{}'.format(column.name, stage, rnd)
            try:
                a, b, fl = rewrite_tg(env, name, t)
            except CodeGenError as e:
                raise CodeGenError("Failed to re-write pipe code '{}' in column '{}.{}': {} "
                                   .format(t, column.table.name, column.name, e))

            cc = str(a)

            if b:
                preamble.append("{} = {} # {}".format(name, b, fl))

        line = line_t.format(cc, fl)

        return line, preamble

    preamble = []

    try_lines = []

    for t in [segment['init'], segment['datatype']] + segment['transforms']:

        if not t:
            continue

        line, col_preamble = make_line(column, t)

        preamble += col_preamble
        try_lines.append(line)

    exception = None
    if segment['exception']:
        exception, col_preamble = make_line(column, segment['exception'])

    if len(try_lines) == 0:
        try_lines.append('pass # Empty pipe segment')

    assert len(try_lines) > 0, column.name

    return preamble, try_lines, exception


def mk_kwd_args(fn, fn_name=None):
    import inspect

    fn_name = fn_name or fn.__name__

    fn_args = inspect.getargspec(fn).args

    if len(fn_args) > 1 and fn_args[0] == 'self':
        args = fn_args[1:]

    kwargs = dict((a, a) for a in all_args if a in args)

    return "{}({})".format(fn_name, ','.join(a + '=' + v for a, v in kwargs.items()))


class ReplaceTG(ast.NodeTransformer):
    """Replace a transform generator with the transform function"""

    def __init__(self, env, tg_name):
        super(ReplaceTG, self).__init__()

        self.tg_name = tg_name
        self.trans_gen = None
        self.env = env
        self.loc = ''

    def missing_args(self):
        pass

    def visit_Call(self, node):

        import inspect
        from ambry.valuetype.types import is_transform_generator
        import types

        if not isinstance(node.func, ast.Name):
            self.generic_visit(node)
            return node

        fn_name = node.func.id
        fn_args = None
        use_kw_args = True

        fn = self.env.get(node.func.id)
        self.loc = file_loc()  # Not a builtin, not a type, not a transform generator

        # In this case, the code line is a type that has a parse function, so rename it.
        if not fn:
            t_fn_name = 'parse_' + fn_name
            t_fn = self.env.get(t_fn_name)
            if t_fn:
                self.loc = file_loc()  # The function is a type
                fn, fn_name = t_fn, t_fn_name

        # Ok, maybe it is a builtin
        if not fn:
            o = eval(fn_name)
            if isinstance(o, types.BuiltinFunctionType):
                self.loc = file_loc()  # The function is a builtin
                fn = o
                fn_args = ['v']
                use_kw_args = False

        if not fn:
            raise CodeGenError("Failed to get function named '{}' from the environment".format(node.func.id))

        if not fn_args:
            fn_args = inspect.getargspec(fn).args

        # Create a dict of the arguments that have been specified
        used_args = dict(tuple(zip(fn_args, node.args))
                         + tuple((kw.arg, kw.value) for kw in node.keywords)
                         )

        # Add in the arguments that were not, but only for args that are specified to be
        # part of the local environment
        for arg in fn_args:
            if arg not in used_args and arg in all_args:
                used_args[arg] = ast.Name(id=arg, ctx=ast.Load())

        # Now, all of the args are in a dict, so we'll re-build them as
        # as if they were all kwargs. Any arguments that were not provided by the
        # signature in the input are added as keywords, with the value being
        # a variable of the same name as the argument: ie. if 'bundle' was defined
        # but not provided, the signature has an added 'bundle=bundle' kwarg

        keywords = [ast.keyword(arg=k, value=v) for k, v in used_args.items()]

        tg_ast = ast.copy_location(
            ast.Call(
                func=ast.Name(id=fn_name, ctx=ast.Load()),
                args=[e.value for e in keywords] if not use_kw_args else [],  # For builtins, which only take one arg
                keywords=keywords if use_kw_args else [],
                starargs=[],
                kwargs=[]
            ), node)

        if is_transform_generator(fn):
            self.loc = file_loc()  # The function is a transform generator.
            self.trans_gen = tg_ast
            replace_node = ast.copy_location(
                ast.Call(
                    func=ast.Name(id=self.tg_name, ctx=ast.Load()),
                    args=[],
                    keywords=[],
                    kwargs=ast.Name(id='col_args', ctx=ast.Load()),
                    starargs=[]
                ), node)

        else:
            replace_node = tg_ast

        return replace_node


def rewrite_tg(env, tg_name, code):
    """Re-write a transform generating function pipe specification by extracting the tranform generating part,
    and replacing it with the generated transform. so:

       tgen(a,b,c).foo.bar

    becomes:

        tg = tgen(a,b,c)

        tg.foo.bar

    """

    visitor = ReplaceTG(env, tg_name)
    assert visitor.tg_name

    tree = visitor.visit(ast.parse(code))

    if visitor.loc:
        loc = ' #' + visitor.loc
    else:
        loc = file_loc()  # The AST visitor didn't match a call node

    if visitor.trans_gen:
        tg = meta.dump_python_source(visitor.trans_gen).strip()
    else:
        tg = None

    return meta.dump_python_source(tree).strip(), tg, loc
