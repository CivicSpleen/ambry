""" Code generation for processing columns

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import ast
import meta

const_args = ('row', 'row_n', 'scratch', 'errors', 'pipe', 'bundle', 'source')
var_args = ('v', 'i_s', 'i_d', 'header_s', 'header_d')
all_args = var_args + const_args

# Full lambda definition for a column, including variable parts
col_code_def = 'lambda {}:'.format(','.join(all_args))

# lambda definition for the who;e row. Includes only the arguments
# that are the same for every column
code_def = 'lambda {}:'.format(','.join(const_args))

column_template="""
def {f_name}(v, i_s, i_d, header_s, header_d, row, row_n, errors, scratch, accumulator, pipe, bundle, source):

    col_args = dict(v=v, i_s=i_s, i_d=i_d, header_s=header_s, header_d=header_d, row=row, row_n=row_n)

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


def base_env():
    """Base environment for evals, the stuff that is the same for all evals"""
    import dateutil.parser
    import datetime
    from functools import partial
    from ambry.valuetype.types import parse_date, parse_time, parse_datetime
    import ambry.valuetype.types
    import ambry.valuetype.math
    import ambry.valuetype.string
    import ambry.valuetype.exceptions
    import ambry.valuetype.test

    localvars = dict(

        parse_date=parse_date,
        parse_time=parse_time,
        parse_datetime=parse_datetime,
        partial=partial,
    )

    localvars.update(dateutil.parser.__dict__)
    localvars.update(datetime.__dict__)
    localvars.update(ambry.valuetype.math.__dict__)
    localvars.update(ambry.valuetype.string.__dict__)
    localvars.update(ambry.valuetype.types.__dict__)
    localvars.update(ambry.valuetype.exceptions.__dict__)
    localvars.update(ambry.valuetype.test.__dict__)

    return localvars

def column_processor_code():
    pass


def find_function(bundle, base_env, code):
    """Look in several places for a caster function:

    - The bundle
    - the ambry.build module, which should be the bundle's module.
    """
    import sys

    if bundle:
        try:
            f = getattr(bundle, code)
            f.ambry_from = '{} = bundle.{}'.format(code, code)
            return f

        except AttributeError:
            pass

        try:
            f = getattr(sys.modules['ambry.build'], code)
            f.ambry_from = 'from ambry.build import {}'.format(code)
            return f

        except AttributeError:
            pass

    try:
        f = base_env[code]
        return f

    except KeyError:
        pass

    raise AttributeError("Could not find caster '{}' in bundle class or bundle module ".format(code))

def make_env(bundle, base_env):

    def _ff(code):
        try:
            return find_function(bundle, base_env, code)
        except (AttributeError, KeyError):
            return None

    return _ff



def make_row_processors(bundle, source_table, dest_table, env = None):
    """
    Make multiple row processors for all of the columns in a table.

    :param source_headers:

    :return:
    """

    dest_headers = [c.name for c in dest_table.columns]
    source_headers = [c.dest_header for c in source_table.columns]

    row_processors = []

    if not env:
        env = make_env(bundle, base_env())

    out = []

    for  i, segments in enumerate(dest_table.transforms):

        seg_funcs = []

        for col_num, (segment, column) in enumerate(zip(segments, dest_table.columns), 1):

            if not segment:
                seg_funcs.append('row[{}], # {}'.format(col_num-1, column.name))
                continue

            assert column.name == segment['column'].name
            col_name = column.name
            preamble, try_lines, exception = make_stack(env, i, segment)

            assert col_num == column.sequence_id, (col_num , column.sequence_id)

            try:
                i_s = source_headers.index(column.name)
                header_s = column.name
                v = 'row[{}]'.format(i_s)

            except ValueError as e:
                i_s = 'None'
                header_s = None
                v = 'None'

            f_name = "{table_name}_{column_name}_{stage}".format(
                table_name=dest_table.name,
                column_name=col_name,
                stage=i
            )

            i_d = column.sequence_id-1

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
                exception=indent + (exception if exception else 'raise'),
                stack='\n'.join(indent+l for l in try_lines)
            )

            seg_funcs.append(f_name
                    +('({v}, {i_s}, {i_d}, {header_s}, \'{header_d}\', '
                      'row, row_n, errors, scratch, accumulator, pipe, bundle, source)')
                    .format(v=v, i_s=i_s, i_d=i_d, header_s="'"+header_s+"'" if header_s else 'None'
                            , header_d=header_d))

            out.append('\n'.join(preamble))

            out.append(column_template.format(**template_args))

        source_headers = dest_headers

        out.append(row_template.format(
            table = dest_table.name,
            stage = i,
            stack = '\n'.join(indent+l+',' for l in seg_funcs)
        ))

        row_processors.append('row_{table}_{stage}'.format(stage=i, table = dest_table.name))

    out.append('row_processors = [{}]'.format(','.join(row_processors)))

    return '\n'.join(out)


def calling_code(f, f_name=None, raise_for_missing=True):
    """Return the code string for calling a function. """
    import inspect
    from ambry.dbexceptions import ConfigurationError

    args = inspect.getargspec(f).args

    if len(args) > 1 and args[0] == 'self':
        args = args[1:]

    for a in args:
        if a not in all_args + ('exception',):  # exception arg is only for exception handlers
            if raise_for_missing:
                raise ConfigurationError('Caster code {} has unknown argument '
                                         'name: \'{}\'. Must be one of: {} '.format(f, a, ','.join(all_args)))

    arg_map = {e:e for e in var_args}

    args = [arg_map.get(a, a) for a in args]

    return "{}({})".format(f_name if f_name else f.__name__, ','.join(args))

def make_stack(env, stage, segment):
    import types
    import collections
    import string
    import random
    from ambry.util import qualified_class_name

    column = segment['column']

    def make_line(column, t):
        preamble = []

        if isinstance(t, type):
            line = "v = {}(v) if v else None".format(t.__name__)
        elif isinstance(object, types.ClassType):
            line = "v = {}(v) if v else None".format(qualified_class_name(t.__name__))
        elif callable(env(t)):
            fn = env(t)

            try:
                frm = fn.ambry_from
            except AttributeError:
                frm = None

            if frm and 'import' in frm:
                preamble.append(frm)
                line = 'v = {}'.format(calling_code(fn, t))
            elif not frm:
                line = 'v = {}'.format(calling_code(fn, t))
            elif 'bundle' in frm:
                line = 'v = bundle.{}'.format(calling_code(fn, t))
            else:
                raise Exception(frm)

        else:

            rnd = (''.join(random.choice(string.ascii_lowercase) for _ in range(6)))

            name = 'tg_{}_{}_{}'.format(column.name, stage, rnd)

            a, b = rewrite_tg(env, name, t)
            line = 'v = {}'.format(a)
            if b:
                preamble.append("{} = {}".format(name, b))

        return line, preamble

    preamble = []

    try_lines = []

    for t in  [segment['init'], segment['datatype'] ] + segment['transforms']:
        if not t:
            continue

        line, col_preamble = make_line(column, t)

        preamble += col_preamble
        try_lines.append(line)

    exception = None
    if segment['exception']:
        exception, col_preamble = make_line(column, segment['exception'])

    return preamble, try_lines, exception


class CodeGenError(Exception):
    pass


def mk_kwd_args(fn, fn_name=None):
    import inspect

    fn_name = fn_name or fn.__name__

    fn_args = inspect.getargspec(fn).args

    if len(fn_args) > 1 and fn_args[0] == 'self':
        args = fn_args[1:]

    kwargs = dict( (a,a) for a in all_args if a in args)

    return "{}({})".format(fn_name, ','.join( a+'='+v for a,v in kwargs.items() ))


class ReplaceTG(ast.NodeTransformer):
    """Replace a transform generator with the transform function"""

    def __init__(self, env, tg_name):
        super(ReplaceTG, self).__init__()

        self.tg_name = tg_name
        self.trans_gen = None
        self.env = env

    def missing_args(self):
        pass

    def visit_Call(self, node):

        import inspect
        from ambry.valuetype.types import is_transform_generator

        if isinstance(node.func, ast.Name):
            fn = self.env(node.func.id)

            if not fn:
                raise CodeGenError("Failed to get function named '{}' from the environment".format(node.func.id))

            fn_args = inspect.getargspec(fn).args

            # Create a dict of the arguments that have been specified
            used_args = dict(tuple(zip(fn_args, node.args))
                            +tuple( (kw.arg, kw.value) for kw in node.keywords)
            )


            # Add in the arguments that were not.
            for arg in fn_args:
                if arg not in used_args:
                    used_args[arg] = ast.Name(id=arg, ctx=ast.Load())

            # Now, all of the args are in a dict, so we'll re-build them as
            # as if they were all kwargs. Any arguments that were not provided by the
            # signature in the input are added as keywords, with the value being
            # a variable of the same name as the argument: ie. if 'bundle' was defined
            # but not provided, the signature has an added 'bundle=bundle' kwarg

            keywords = [ast.keyword(arg=k, value=v) for k, v in used_args.items()]

            tg_ast = ast.copy_location(
                ast.Call(
                    func=ast.Name(id=node.func.id, ctx=ast.Load()),
                    args=[],
                    keywords=keywords,
                    starargs=[],
                    kwargs=[]
                ), node)

            if is_transform_generator(fn):
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

        else:
            self.generic_visit(node)
            return node

def rewrite_tg(env, tg_name, code):

    visitor = ReplaceTG(env, tg_name)
    assert visitor.tg_name
    tree = visitor.visit(ast.parse(code))

    if visitor.trans_gen:
        tg = meta.dump_python_source(visitor.trans_gen).strip()
    else:
        tg = None

    return meta.dump_python_source(tree).strip(), tg