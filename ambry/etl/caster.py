"""Support functions for transforming rows read from input files before writing
to databases.

Copyright (c) 2015 Civic Knowlege. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""


from .pipeline import  CodeCallingPipe


class CasterPipe(CodeCallingPipe):

    def __init__(self, table=None, error_handler=None):
        import sys

        self.errors = []
        self.table = table

        self.row_processor = None

        self.types = []
        self._compiled = None

        self.error_handler = error_handler

        self.error_accumulator = None

        self.transform_code = ''
        self.col_code = []

        self.new_headers = None
        self.orig_headers = None

        super(CasterPipe,self).__init__()

        for k,v in sys.modules[__name__].__dict__.items():
            if callable(v):
                self.add_to_env(v)

    def cast_error(self, errors, type_, name, v, e):
        errors[name] = {'type': type_, 'value': v, 'exception': str(e)}



    def process_header(self, header):

        from ambry.etl import RowProxy
        from ambry.valuetype import import_valuetype

        from ambry.dbexceptions import ConfigurationError

        # The definition line for the lambda functions in which casters are executed
        lambda_def = "lambda row, v, errors, pipe=self, i=i, header=header, bundle=self.bundle, source=self.source:"

        if self.table:
            table = self.table
        else:
            self.table = table = self.source.dest_table

        env = {}
        row_parts = []

        col_code = [None]*len(self.table.columns)

        # Create an entry in the row processor function for each output column in the schema.
        for i,c in enumerate(self.table.columns):

            f_name = "f_"+str(c.name)

            type_f = c.valuetype_class

            if c.name not in header:

                # There is no source column, so insert a None. Maybe this should be an error.
                env[f_name] = lambda row, v, caster=self, i=i, header=c.name: None

                col_code[i]  = (c.name,"None")

            elif c.caster:

                if type_f.__name__ != c.datatype:
                    raise ConfigurationError("Can't have custom datatype with caster, in table {}.{}"
                                             .format(c.table.name, c.name))

                # Regular casters, from the "caster" column of the schema
                try:
                    caster_f = self.get_caster_f(c.caster)

                    # The inspection will call the caster_f with the argument list declared in its defintion,
                    # so the dfinition just has to have the same names are appear in the argument list to the
                    # lambda
                    code = self.calling_code(caster_f, c.caster)

                    self.add_to_env(caster_f)

                except AttributeError:
                    # The caster isn't a name of a function on the bundle nor the bundle module,
                    # so guess that it is code to eval.

                    code = c.caster

                # Wrap the code to cast it to the final datatype
                wrapped_code = lambda_def+"parse_{}(pipe, header, ({}), row, errors)".format(type_f.__name__,code)

                env[f_name] = eval(wrapped_code,  self.env(i=i, header=c.name) )

                col_code[i] = (c.name,code)

            elif type_f.__name__ == c.datatype:
                # Plain python type, from the "datatype" column

                env[f_name] = eval(lambda_def+"parse_{}(pipe, header, v, row, errors)"
                                   .format(type_f.__name__), self.env(i=i, header=c.name))

                col_code[i] = (c.name, "parse_{}(caster, header, v, errors)".format(type_f.__name__) )

            else:

                # Special valuetype, not a normal python type
                vt_name = c.datatype.replace('.','_')
                self.add_to_env(import_valuetype(c.datatype), vt_name)
                env[f_name] = eval(lambda_def+" parse_type({},pipe, header, v, row)"
                                    .format(vt_name), self.env(i=i, header=c.name))

                col_code[i] = (c.name, "parse_type({},caster, header, v, row, errors)".format(c.datatype.replace('.','_')))

            self.add_to_env(env[f_name], f_name)

            try:
                header_index = header.index(c.name)
            except ValueError:
                header_index = None

            row_parts.append((f_name, header_index))

        self.col_code = col_code

        inner_code = ','.join(["{}(row, row[{}], errors)".format(f_name, index) if index != None else "None"
                               for (f_name, index) in row_parts])

        self.transform_code = "lambda row, errors: [{}] ".format(inner_code)

        self.row_processor = eval(self.transform_code, self.env())

        self.orig_headers = header
        # Return the table header, rather than the original row header.
        self.new_headers =  [ c.name for c in self.table.columns ]

        self.row_proxy = RowProxy(self.orig_headers)

        return self.new_headers

    def process_body(self, row):

        errors = {}  # Clear the accumulator

        if len(row) != len(self.orig_headers):
            raise CasterError('Header has {} items, Row has {} items\nheaders= {}'
                              '\nrow    = {}'
                              .format(len(self.orig_headers), len(row),self.orig_headers,row))

        row = self.row_processor(self.row_proxy.set_row(row), errors)

        if self.error_handler and errors:
            self.error_handler(row, errors)
        else:
            self.errors.append(errors)

        return row

    def __str__(self):
        from ambry.util import qualified_class_name

        col_codes = '\n'.join( '  {:2d} {:15s}: {}'.format(i,col, e) for i, (col,e) in enumerate(self.col_code))

        return (qualified_class_name(self) + "\n" +
                self.indent + "Code: "+ self.transform_code + "\n" +col_codes)
