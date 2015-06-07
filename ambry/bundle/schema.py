"""The schema sub-object provides acessors to the schema for a bundle. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from collections import OrderedDict, defaultdict

from ambry.dbexceptions import ConfigurationError
from ambry.orm import Column
from ambry.orm.exc import NotFoundError
from ambry.util import memoize

PROTO_TERMS = 'civicknowledge.com-proto-proto_terms'

def _clean_flag(in_flag):
    if in_flag is None or in_flag == '0':
        return False

    return bool(in_flag)


def _clean_int(i):
    if isinstance(i, int):
        return i
    elif isinstance(i, basestring):
        if len(i) == 0:
            return None

        return int(i.strip())
    elif i is None:
        return None
        # raise ValueError("Input must be convertable to an int. got:  ".str(i))


class Schema(object):
    """Represents the table and column definitions for a bundle
    """

    def __init__(self, bundle):
        from bundle import Bundle

        self.bundle = bundle  # COuld also be a partition

        self.d_id = self.bundle.dataset.id
        self.d_vid = self.bundle.dataset.vid

        self._seen_tables = {}
        self.table_sequence = None
        self.max_col_id = {}

        # Cache for references to code tables. 
        self._code_table_cache = None

        # Flag to indicate that new code tables were added, so the
        # build should be re-run
        self.new_code_tables = False

    @property
    def tables(self):
        '''Return a list of tables for this bundle'''
        # from ambry.orm import Table

        from ambry.orm import Table

        return self.bundle.dataset.tables

    @property
    def columns(self):
        '''Return a list of table columns for this bundle'''
        from ambry.orm import Column

        return self.bundle.database.session.query(Column).all()

    def read(self):
        """Read a CSV file, in a particular format, to generate the schema"""
        from ..orm.file import File

        bsfile = self.bundle.dataset.bsfile(File.BSFILE.SCHEMA)

        for row in bsfile.dict_row_reader:
            print row

        return

        import re

        t = None

        new_table = True
        last_table = None
        line_no = 1  # Accounts for file header. Data starts on line 2

        errors = []
        warnings = []

        extant_tables = [t.name for t in self.tables]

        reader = None

        with self.bundle.session:
            for row in reader:

                line_no += 1

                if not row.get('column', False) and not row.get('table', False):
                    continue

                row = {k: str(v).decode('utf8', 'ignore').encode('ascii', 'ignore').strip()
                       for k, v in row.items()}

                if row['table'] and row['table'] != last_table:
                    new_table = True
                    last_table = row['table']

                if new_table and row['table']:

                    if row['table'] in extant_tables:
                        errors.append((row['table'], None, "Table already exists"))
                        return warnings, errors

                    try:
                        table_row = dict(**row)
                        del table_row[
                            'type']  # The field is really for columns, and means something different for tables

                        t = self.add_table(row['table'], **table_row)
                    except Exception as e:
                        errors.append((None, None, " Failed to add table: {}. Row={}. Exception={}".format(row['table'],
                                                                                                           dict(row),
                                                                                                           e)))
                        return warnings, errors

                    new_table = False

                # Ensure that the default doesnt get quotes if it is a number.
                if row.get('default', False):
                    try:
                        default = int(row['default'])
                    except:
                        default = row['default']
                else:
                    default = None

                if not row.get('column', False):
                    raise ConfigurationError("Row error: no column on line {}".format(line_no))
                if not row.get('table', False):
                    raise ConfigurationError("Row error: no table on line {}".format(line_no))
                if not row.get('type', False):
                    raise ConfigurationError("Row error: no type on line {}".format(line_no))

                indexes = [row['table'] + '_' + c for c in row.keys() if (re.match('i\d+', c) and _clean_flag(row[c]))]
                uindexes = [row['table'] + '_' + c for c in row.keys() if
                            (re.match('ui\d+', c) and _clean_flag(row[c]))]
                uniques = [row['table'] + '_' + c for c in row.keys() if (re.match('u\d+', c) and _clean_flag(row[c]))]

                datatype = row['type'].strip().lower()

                width = _clean_int(row.get('width', None))
                size = _clean_int(row.get('size', None))
                start = _clean_int(row.get('start', None))

                data = {k.replace('d_', '', 1): v for k, v in row.items() if k.startswith('d_')}

                description = row.get('description', '').strip().encode('utf-8')

                col = self.add_column(
                    t, row['column'], sequence_id=row.get('seq', None),
                    is_primary_key=True if row.get('is_pk', False) else False,
                    fk_vid=row['is_fk'] if row.get('is_fk', False) else None, description=description,
                    datatype=datatype, proto_vid=row.get('proto_vid'), derivedfrom=row.get('derivedfrom'),
                    unique_constraints=','.join(uniques), indexes=','.join(indexes), uindexes=','.join(uindexes),
                    default=default, size=size, start=start, width=width, data=data,
                    sql=row.get('sql'), precision=int(row['precision']) if row.get('precision', False) else None,
                    scale=float(row['scale']) if row.get('scale', False) else None, flags=row.get('flags', None),
                    keywords=row.get('keywords'), measure=row.get('measure'), units=row.get('units', None),
                    universe=row.get('universe'), commit=False)

                if col:
                    self.validate_column(t, col, warnings, errors)
        return warnings, errors

    @classmethod
    def get_table_from_database(cls, db, name_or_id, session=None, d_vid=None):
        '''Return the orm.Table record from the bundle schema '''
        from ambry.orm.exc import NotFoundError
        from ambry.orm.table import Table

        import sqlalchemy.orm.exc
        from sqlalchemy.sql import or_, and_

        if not name_or_id:
            raise ValueError("Got an invalid argument for name_or_id: '{}'".format(name_or_id))

        Table.mangle_name(name_or_id)

        try:
            if d_vid:
                return session.query(Table).filter(
                    and_(Table.d_vid == d_vid, or_(Table.vid == name_or_id, Table.id_ == name_or_id,
                                                   Table.name == Table.mangle_name(name_or_id)))
                ).one()

            else:
                return session.query(Table).filter(or_(Table.vid == name_or_id, Table.id_ == name_or_id,
                                                       Table.name == Table.mangle_name(name_or_id))).one()

        except sqlalchemy.orm.exc.NoResultFound:
            raise NotFoundError("No table for name_or_id: '{}'".format(name_or_id))



    def remove_table(self, table_name):
        from ..orm import Table, Column
        from sqlalchemy.orm.exc import NoResultFound

        s = self.bundle.database.session

        try:
            table = (s.query(Table).filter(Table.name == table_name)).one()
        except NoResultFound:
            table = None

        if not table:
            return

        s.query(Column).filter(Column.t_vid == table.vid).delete()
        s.query(Table).filter(Table.vid == table.vid).delete()

        s.commit()

        if table_name in self._seen_tables:
            del self._seen_tables[table_name]



    @classmethod
    def validate_column(cls, table, column, warnings, errors):

        from ..identity import ObjectNumber

        # Postgres doesn't allow size modifiers on Text fields.
        if column.datatype == Column.DATATYPE_TEXT and column.size:
            warnings.append(
                (table.name, column.name, "Postgres doesn't allow a TEXT field to have a size. Use a VARCHAR instead."))

        # MySql requires that text columns that have a default also have a size. 
        if column.type_is_text() and bool(column.default):
            if not column.size and not column.width:
                warnings.append(
                    (table.name, column.name, "MySql requires a Text or Varchar field with a default to have a size."))

            if isinstance(column.default, basestring) and column.width and len(column.default) > column.width:
                warnings.append((table.name, column.name, "Default value is longer than the width"))

            if isinstance(column.default, basestring) and column.size and len(column.default) > column.size:
                warnings.append((table.name, column.name, "Default value is longer than the size"))

        if column.default:
            try:
                column.python_cast(column.default)
            except TypeError as e:
                errors.append((table.name, column.name,
                               "Bad default value '{}' for type '{}' (T); {}".format(column.default, column.datatype,
                                                                                     e)))
            except ValueError:
                errors.append((table.name, column.name,
                               "Bad default value '{}' for type '{}' (V)".format(column.default, column.datatype)))

        if column.fk_vid and ObjectNumber.parse(column.fk_vid).revision:
            errors.append((table.name, column.name, "Foreign key can't have a revision number"))

    @classmethod
    def translate_type(cls, driver, table, column):
        '''Translate types for particular driver, and perform some validity checks'''
        # Creates a lot of unnecessary objects, but speed is not important here.  

        if driver == 'postgis':
            driver = 'postgres'

        if driver == 'mysql':

            if (column.datatype in (Column.DATATYPE_TEXT, column.datatype == Column.DATATYPE_VARCHAR) and
                    bool(column.default) and not bool(column.size) and not bool(column.width)):
                raise ConfigurationError("Bad column {}.{}: For MySql, text columns with default must "
                                         "also have size or width".format(table.name, column.name))

            if column.datatype in (Column.DATATYPE_TEXT, column.datatype == Column.DATATYPE_VARCHAR) \
                    and bool(column.default) and not bool(column.size) and bool(column.width):
                column.size = column.width

            # Mysql, when running on Windows, does not allow default
            # values for TEXT columns
            if column.datatype == Column.DATATYPE_TEXT and bool(column.default):
                column.datatype = Column.DATATYPE_VARCHAR

            # VARCHAR requires a size
            if column.datatype == Column.DATATYPE_VARCHAR and not bool(column.size):
                column.datatype = Column.DATATYPE_TEXT

                # Postgres doesn't allows size specifiers in TEXT columns.
        if driver == 'postgres':
            if column.datatype == Column.DATATYPE_TEXT and bool(column.size):
                column.datatype = Column.DATATYPE_VARCHAR

        if driver == 'sqlite' or driver != 'postgres':
            if column.is_primary_key and column.datatype == Column.DATATYPE_INTEGER64:
                column.datatype = Column.DATATYPE_INTEGER  # Required to trigger autoincrement

        type_ = Column.types[column.datatype][0]

        if column.datatype == Column.DATATYPE_NUMERIC:
            return type_(column.precision, column.scale)
        elif column.size and column.datatype != Column.DATATYPE_INTEGER:
            try:
                return type_(column.size)
            except TypeError:  # usually, the type does not take a size
                return type_
        else:
            return type_

    @staticmethod
    def munge_index_name(table, n, alt=None):
        if alt:
            return alt + '_' + n
        else:
            return str(table.vid) + '_' + n

    def get_table_meta(self, name_or_id, use_id=False, driver=None, alt_name=None):
        """Method version of get_table_meta_from_db"""
        return self.get_table_meta_from_db(self.bundle.database, name_or_id, use_id, driver,
                                           session=self.bundle.database.session, alt_name=alt_name)

    @classmethod
    def get_table_meta_from_db(cls, db, name_or_id, use_id=False, driver=None, d_vid=None, session=None, alt_name=None,
                               use_fq_col_names=False):
        """
            use_id: prepend the id to the class name
        """

        from sqlalchemy import MetaData, UniqueConstraint, Index, text
        from sqlalchemy import Column as SAColumn
        from sqlalchemy import Table as SATable
        from ..orm.exc import NotFoundError


        if use_fq_col_names:
            def col_name(c):
                return c.fq_name
        else:
            def col_name(c):
                return c.name

        metadata = MetaData()

        try:
            table = cls.get_table_from_database(db, name_or_id, d_vid = d_vid, session=session)
        except NotFoundError:
            raise NotFoundError("Did not find table '{}' in database {}".format(name_or_id, db.dsn))

        if alt_name and use_id:
            raise ConfigurationError("Can't specify both alt_name and use_id")

        if alt_name:
            table_name = alt_name
        elif use_id:
            table_name = table.vid.replace('/', '_') + '_' + table.name
        else:
            table_name = table.name

        at = SATable(table_name, metadata)

        indexes = {}
        uindexes = {}
        constraints = {}
        foreign_keys = {}

        assert len(table.columns) > 0, "Tables can't have 0 columns: '{}'".format(table_name)

        for column in table.columns:

            kwargs = {}

            # width = column.size if column.size else (column.width if column.width else None)

            if column.default is not None:

                try:
                    int(column.default)
                    kwargs['server_default'] = text(str(column.default))
                except:

                    kwargs['server_default'] = column.default

            tt = cls.translate_type(driver, table, column)

            ac = SAColumn(col_name(column), tt, primary_key=(column.is_primary_key == 1), **kwargs)

            at.append_column(ac)

            # assemble non unique indexes
            if column.indexes and column.indexes.strip():
                for cons in column.indexes.strip().split(','):
                    if cons.strip() not in indexes:
                        indexes[cons.strip()] = []
                    indexes[cons.strip()].append(ac)

            # assemble  unique indexes
            if column.uindexes and column.uindexes.strip():
                for cons in column.uindexes.strip().split(','):
                    if cons.strip() not in uindexes:
                        uindexes[cons.strip()] = []
                    uindexes[cons.strip()].append(ac)

            # Assemble constraints
            if column.unique_constraints and column.unique_constraints.strip():
                for cons in column.unique_constraints.strip().split(','):

                    if cons.strip() not in constraints:
                        constraints[cons.strip()] = []

                    constraints[cons.strip()].append(ac)

        # Append constraints.
        for constraint, columns in constraints.items():
            at.append_constraint(
                UniqueConstraint(name=cls.munge_index_name(table, constraint, alt=alt_name), *columns))

        # Add indexes   
        for index, columns in indexes.items():
            Index(cls.munge_index_name(table, index, alt=alt_name), unique=False, *columns)

        # Add unique indexes   
        for index, columns in uindexes.items():
            Index(cls.munge_index_name(table, index, alt=alt_name), unique=True, *columns)

        return metadata, at

    def generate_indexes(self, table):
        """Used for adding indexes to geo partitions. Generates index CREATE commands"""

        indexes = {}
        uindexes = {}

        for column in table.columns:
            # assemble non unique indexes
            if column.indexes and column.indexes.strip():
                for cons in column.indexes.strip().split(','):
                    if cons.strip() not in indexes:
                        indexes[cons.strip()] = set()
                    indexes[cons.strip()].add(column)

            # assemble  unique indexes
            if column.uindexes and column.uindexes.strip():
                for cons in column.uindexes.strip().split(','):
                    if cons.strip() not in uindexes:
                        uindexes[cons.strip()] = set()
                    uindexes[cons.strip()].add(column)

        for index_name, cols in indexes.items():
            index_name = self.munge_index_name(table, index_name)
            yield "CREATE INDEX IF NOT EXISTS {} ON {} ({});".format(index_name, table.name,
                                                                     ','.join([c.name for c in cols]))

        for index_name, cols in uindexes.items():
            index_name = self.munge_index_name(table, index_name)
            yield "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});".format(index_name, table.name,
                                                                            ','.join([c.name for c in cols]))

    def create_tables(self):
        """Create the defined tables as database tables."""
        with self.bundle.session:
            for t in self.tables:
                if t.name not in self.bundle.database.inspector.get_table_names():
                    t_meta, table = self.bundle.schema.get_table_meta(t.name)  # @UnusedVariable
                    table.create(bind=self.bundle.database.engine)

    def caster(self, table_name):
        """Return a caster for a table. This is like orm.Table.caster, but it will use special caster types
        defined in the schema"""

        from ambry.transform import CasterTransformBuilder

        table = self.table(table_name)

        bdr = CasterTransformBuilder()

        for c in table.columns:

            # Try to get a caster type object from the bundle
            if 'caster' in c.data and c.data['caster']:
                t = None

                for l in [self.bundle, self.bundle.__module__]:
                    try:
                        t = getattr(self.bundle, c.data['caster'])
                        bdr.add_type(t)
                        break
                    except AttributeError:
                        continue

                if not t:
                    self.bundle.error("Schema declared undefined caster type {} for {}.{}. Ignoring"
                                      .format(c.data['caster'], table.name, c.name))
                    t = c.python_type

            else:
                t = c.python_type

            bdr.append(c.name, t)

        return bdr


    def expand_table_prototypes(self):
        """Look for tables that have prototypes, get the original table, and expand the
        local definition to include all of the prototypes's columns"""
        from ..orm import Table
        from ..orm.exc import NotFoundError
        from ..identity import ObjectNumber

        q = self.bundle.database.session.query(Table).filter(Table.proto_vid != None)

        l = self.bundle.library

        def table_protos():

            for t in q.all():

                if not t.proto_vid:
                    continue

                for proto in t.proto_vid.split(','):
                    yield (t, proto.strip())

        for t, proto_vid in table_protos():

            try:
                proto_table = l.table(proto_vid)
            except NotFoundError:
                self.bundle.error("Can't expand prototype for table {}: missing prototype reference {} (a)"
                                  .format(t.name, proto_vid))
                continue

            if not proto_table:
                self.bundle.error("Can't expand prototype for table {}: missing prototype reference {} (b)"
                                  .format(t.name, proto_vid))
                continue

            # t_on = ObjectNumber.parse(proto_vid)
            ObjectNumber.parse(proto_vid)

            for c in proto_table.columns:

                if c.name == 'id':
                    # This column already exists, not re-adding it preserves numbering
                    idc = t.column('id')
                    idc.description = proto_table.description
                    idc.proto_vid = proto_vid
                    idc.datatype = c.datatype
                    idc.is_primary_key = True

                else:

                    d = c.dict

                    del d['id_']
                    del d['vid']
                    del d['t_id']
                    del d['t_vid']
                    name = d['name']
                    del d['name']
                    del d['sequence_id']

                    # Protovids never have the version -- they arent really vids
                    d['proto_vid'] = c.id_

                    self.add_column(t, name, **d)

    @property
    @memoize
    def prototype_map(self):

        pt = self.bundle.library.get(PROTO_TERMS).partition

        self.bundle.log("Loading protos from {}".format(pt.identity.fqname))
        pt_map = {row['name']: dict(row) for row in pt.rows}

        return pt_map

    def expand_column_prototypes(self):
        """Find values for the proto_vid that are in the proto_terms dataset, and expand them into column vids

        :return:
        """
        from ..orm import Column
        from ..identity import ObjectNumber, NotObjectNumberError
        from collections import defaultdict
        from ..orm.exc import NotFoundError

        q = (self.bundle.database.session.query(Column)
             .filter(Column.proto_vid != None).filter(Column.proto_vid != ''))


        # Group expanded columns by souce table, to create sql indexes for the sets of columns
        # pointing ot the same ambry index dataset
        table_cols = defaultdict(set)

        for c in q.all():

            try:
                ObjectNumber.parse(c.proto_vid)
                # Its all good. The proto_vid is an Object, not a name.

                continue # Explicit, but not necessary.

            except NotObjectNumberError:

                # If the proto_vid isn't a valid number, it is probably a proto string, from
                # the proto_terms table.

                pt_map = self.prototype_map

                try:
                    pt_row = pt_map[c.proto_vid]
                except KeyError:
                    self.bundle.error("Proto vid for {}.{} is not defined: {} ".format(c.table.name, c.name, c.proto_vid))
                    continue

                c.data['orig_proto_vid'] = c.proto_vid
                c.proto_vid = pt_row['obj_number']

                if c.datatype != pt_row['datatype']:
                    self.bundle.error(("Column datatype for {}.{} doesn't match prototype: "
                                              "{} != {} ").format(c.table.name, c.name, c.datatype, pt_row['datatype']))


                # At this point, we've converted the proto_vid from a proto_term string to an Object NUmber,
                # now we can link up an index if it is defined. If the index parition exists, then
                # we look for the new proto-vid object number in the table of the index partition,
                # and connect the foreign keys if there is a match.

                index_partition = pt_row['index_partition']

                if index_partition:
                    try:
                        # FIXME! Should use library.column() instead
                        ip = self.bundle.library.get(index_partition).partition
                    except NotFoundError:
                        self.bundle.error("Failed to get index '{}' while trying to check index coverage"
                                          .format(index_partition, c.table.name, c.name ))
                        continue

                    for ipc in ip.table.columns:

                        if ipc.proto_vid == c.proto_vid:
                            c.fk_vid = ipc.id_
                            c.data['index'] = "{}".format(str(ip.identity.vid))
                            c.data['index_name'] = "{}:{}".format(str(ip.identity.vname), ipc.name)

                            self.bundle.log("expand_column_prototype: {}.{} -> {}.{}".format(c.table.name, c.name,
                                str(ip.identity.vname), ipc.name))

                            table_cols[ip.identity.vid].add(c.vid)

        # Now check that for each table, the table has columns that link to all of the  link column in the index.
        for t in self.tables:

            indexes = set([ c.data['index'].split(':')[0] for c in t.columns if 'index' in c.data and c.data['index']])

            for index in indexes:
                try:
                    ip = self.bundle.library.get(index).partition
                except NotFoundError:
                    self.bundle.error(("Failed to get index '{}' while trying to expand proto_id for column"
                                       " {}.{} ").format(index, c.table.name, c.name))
                    continue

                index_columns =  set([ str(c.id_) for c in ip.table.columns if c.name != 'id'])

                link_columns =  set([ str(a.id_) for a,b  in ip.table.link_columns(t) ])

                diff = index_columns.difference(link_columns)

                if diff:
                    missing_cols = ', '.join(ip.table.column(c).name for c in diff)
                    self.bundle.warn('Table {} does not cover all of the index values for index {}; missing {}'
                                     .format(t.name, ip.vname, missing_cols)  )


    def extract_schema(self, db):
        '''Extract an Ambry schema from a database and create it in this bundle '''

        for table_name in db.inspector.get_table_names():
            self.bundle.log("Extracting: {}".format(table_name))

            t = self.add_table(table_name)

            for c in db.inspector.get_columns(table_name):

                name = c['name']

                try:
                    size = c['type'].length
                    dt = str(c['type']).replace(str(size), '').replace('()', '').lower()
                except AttributeError:
                    size = None
                    dt = str(c['type']).lower()

                self.bundle.log("   {} {} {} ".format(name, dt, size))

                self.add_column(t, name, datatype=dt,
                                size=size,
                                is_primary_key=c['primary_key'] != 0)

    def write(self):
        '''Write the schema back to the schema file'''
        with open(self.bundle.filesystem.path('meta', self.bundle.SCHEMA_FILE), 'w') as f:
            self.as_csv(f)

    def copy_table(self, in_table, out_table_name=None):
        '''Copy a table schema into this schema
      
        '''

        if not out_table_name:
            out_table_name = in_table.name

        with self.bundle.session:  # as s:

            cols = []
            for c in in_table.columns:
                d = c.dict

                del d['t_vid']
                del d['vid']
                del d['sequence_id']
                cols.append(d)

            table = self.add_table(out_table_name, data=in_table.data)
            for c in cols:
                self.add_column(table, **c)

            return table

    @staticmethod
    def _dump_gen(self, table_name=None):
        """Yield schema row for use in exporting the schema to other
        formats

        """

        # Collect indexes
        indexes = {}

        # Sets the order of the fields
        all_opt_col_fields = ["size", "precision", "scale", "default", "start", "width",
                              "description", "sql", "flags", "keywords",
                              "measure", "units", "universe", 'proto_vid', "derivedfrom"]

        # Collects what fields actually exist
        opt_fields_set = set()

        all_opt_table_fields = ["keywords", "universe"]

        if table_name:
            tables = [table for table in self.tables if table.name == table_name]
        else:
            tables = self.tables

        data_fields = set()
        # Need to get all of the indexes figured out first, since there are a variable number of indexes.
        for table in tables:

            if table.proto_vid:
                opt_fields_set.add("proto_vid")

            for field in all_opt_table_fields:

                v = getattr(table, field)
                if v and field not in opt_fields_set:
                    opt_fields_set.add(field)

            for col in table.columns:

                if col.proto_vid:
                    opt_fields_set.add("proto_vid")

                for index_set in [col.indexes, col.uindexes, col.unique_constraints]:
                    if not index_set:
                        continue  # HACK. This probably should not happen

                    for idx in index_set.split(','):

                        idx = idx.replace(table.name + '_', '')
                        if idx not in indexes:
                            indexes[idx] = set()

                        indexes[idx].add(col)

                for field in all_opt_col_fields:

                    v = getattr(col, field)
                    if v and field not in opt_fields_set:
                        opt_fields_set.add(field)

                for k, v in col.data.items():
                    data_fields.add(k)

                    # also add data columns for the table

            for k, v in table.data.items():
                data_fields.add(k)

        data_fields = sorted(data_fields)

        # Put back into same order as in all_opt_col_fields
        opt_col_fields = [field for field in all_opt_col_fields if field in opt_fields_set]

        indexes = OrderedDict(sorted(indexes.items(), key=lambda t: t[0]))

        first = True

        for table in tables:

            for col in table.columns:
                row = OrderedDict()
                row['table'] = table.name
                row['seq'] = col.sequence_id
                row['column'] = col.name
                row['is_pk'] = 1 if col.is_primary_key else ''
                row['is_fk'] = col.fk_vid if col.fk_vid else None
                row['id'] = None
                row['type'] = col.datatype.upper() if col.datatype else None

                for idx, s in indexes.items():
                    if idx:
                        row[idx] = 1 if col in s else None

                for field in opt_col_fields:
                    row[field] = getattr(col, field)

                if col.is_primary_key:
                    # For the primary key, the data comes from the table. 
                    for k in data_fields:
                        row['d_' + k] = table.data.get(k, None)

                    # In CSV files the table description is stored as the description of the
                    # id column
                    if not col.description and table.description:
                        col.description = table.description

                else:
                    for k in data_fields:
                        row['d_' + k] = col.data.get(k, None)

                row['description'] = col.description

                # The primary key is special. It is always first and it always exists,
                # so it can hold the id of the table instead. ( The columns's id field is not first,
                # but the column record for the tables id field is first.
                if row['is_pk']:
                    row['id'] = table.id_
                    if table.proto_vid:
                        row['proto_vid'] = table.proto_vid

                    for field in all_opt_table_fields:

                        v = getattr(table, field)

                        if v and field in opt_fields_set:
                            row[field] = v

                else:
                    row['id'] = col.id_
                    if col.proto_vid:
                        row['proto_vid'] = col.proto_vid

                if first:
                    first = False
                    yield row.keys()

                yield row

    def as_csv(self, f=None):
        import unicodecsv as csv
        from StringIO import StringIO

        if f is None:
            f = StringIO()

        g = self._dump_gen(self)

        try:
            header = g.next()
        except StopIteration:
            # No schema file at all!
            return

        w = csv.DictWriter(f, header, encoding='utf-8')
        w.writeheader()
        last_table = None
        for row in g:

            # Blank row to seperate tables. 
            if last_table and row['table'] != last_table:
                w.writerow({})

            w.writerow(row)

            last_table = row['table']

        if isinstance(f, StringIO):
            return f.getvalue()


    def write_codes(self):

        import unicodecsv as csv

        header = ['table', 'column', 'key', 'value', 'description']

        count = 0

        with open(self.bundle.filesystem.path('meta', self.bundle.CODE_FILE), 'w') as f:

            w = csv.writer(f, encoding='utf-8')
            w.writerow(header)
            for t in self.tables:
                for c in t.columns:
                    for cd in c.codes:
                        row = [ t.name,c.name,cd.key,cd.value,cd.description]

                        w.writerow(row)
                        count += 1

        if not count:
            import os

            os.remove(self.bundle.filesystem.path('meta', self.bundle.CODE_FILE))

    def read_codes(self):
        """Read codes from a codes.csv file back into the schema"""
        import csv
        from ..orm.exc import NotFoundError

        with  open(self.bundle.filesystem.path('meta', self.bundle.CODE_FILE), 'r') as f:

            r = csv.DictReader(f)
            table = None
            column = None
            for row in r:

                if not row['table'] or not row['column'] or not row['key']:

                    continue

                try:
                    if not table or table.name != row['table']:
                        table = self.table(row['table'])
                        column = None

                    if not column or column.name != row['column']:
                        column = table.column(row['column'])

                    column.add_code(row['key'], row['value'], row['description'])
                except NotFoundError as e:
                    self.bundle.error("Skipping code '{}' for {}.{} : {}"
                                      .format(row['key'],row['table'],row['column'], e))


    @property
    def dict(self):
        """Represent the entire schema as a dict, suitable for conversion to json"""
        s = {}

        for t in self.tables:
            s[t.vid] = t.nonull_col_dict

        return s

    def _repr_html_(self):

        out = ''

        for t in self.tables:
            out += '\n<h2>{}</h2>\n'.format(t.name)
            out += t._repr_html_()

        return out

    def add_views(self):
        """Add views defined in the configuration"""

        for p in self.bundle.partitions:

            if not p.table:
                continue

            if not self.bundle.config.group('views'):
                raise ConfigurationError('add_views() requires views to be specified in the configuration file')

            views = self.bundle.config.views.get(p.table.name, False)

            if not views:
                continue

            for name, view in views.items():
                self.bundle.log("Adding view: {} to {}".format(name, p.identity.name))
                sql = "DROP VIEW IF EXISTS {}; ".format(name)
                p.database.connection.execute(sql)

                sql = "CREATE VIEW {} AS {};".format(name, view)
                p.database.connection.execute(sql)

    def update_lengths(self, table_name, lengths):
        '''Update the sizes of the columns in table with a dict mapping column names to length'''

        with self.bundle.session as s:

            table = self.table(table_name)

            for c in table.columns:

                size = lengths.get(c.name, False)

                if size and size > c.size:
                    self.bundle.log(
                        "Updating schema column length {}.{} {} -> {}".format(table.name, c.name, c.size, size))
                    c.size = size

                    # Integers that are too long for 32 bits should be upgraded to 64
                    if c.datatype == c.DATATYPE_INTEGER and c.size >= 10:  # 2^32 is 10 gigits
                        self.bundle.log(
                            "Updating schema column datatype {}.{} to integer64".format(table.name, c.name, c.size,
                                                                                        size))
                        c.datatype = c.DATATYPE_INTEGER64

                    s.merge(c)

        # Need to expire the unmanaged cache, or the regeneration of the schema in _revise_schema will
        # use the cached schema object rather than the ones we just updated. 
        self.bundle.database.session.expire_all()


    #
    # Updating Schemas
    #
    #

    def update_from_intuiter(self, table_name, intuiter, logger=None):
        """
        Update a table schema using a memo from intuit()

        :param table_name:
        :param intuiter:
        :param logger:
        # :param description: Table description
        # :param descriptions:  Dict apping column names to column descriptions
        :return:
        """
        from datetime import datetime, time, date
        # import string
        import re

        type_map = {int: 'integer', str: 'varchar', float: 'real',
                    datetime: 'datetime', date: 'date', time: 'time'}

        # ok_chars = string.digits + string.letters + string.punctuation + ' '

        with self.bundle.session:  # as s:

            table = self.table(table_name)

            for col in intuiter.columns:
                name = col.name

                if name == 'id':
                    self.add_column(table, 'id', datatype='integer', is_primary_key=True)

                else:

                    type_, has_codes = col.resolved_type()

                    description = re.sub('[\r\n\s]+', ' ', col.description).strip() if col.description else ''

                    # add_column will update existing columns
                    orm_col = self.add_column(
                        table, name, datatype=type_map[type_], description=description,
                        size=col.length if type_ == str else None, data=dict(has_codes=1) if has_codes else {})


        self.write_schema()

    def update_from_iterator(self, table_name, iterator, header=None,
                             max_n=None, logger=None):
        """

        :param table_name:
        :param iterator:
        :param header: If list, a list of columns names. If an OrderedDict, the keys are the column
        names, and the values are column descriptions.
        :param max_n:
        :param logger:
        :return:
        """
        from util.intuit import Intuiter

        from collections import OrderedDict

        assert isinstance(header, (type(None), OrderedDict, list, tuple))

        if header and isinstance(header, OrderedDict):
            descriptions = header
            header = header.keys()
        else:
            descriptions = None

        intuit = Intuiter(header=header, logger=logger)
        intuit.iterate(iterator, max_n=max_n)

        self.update_from_intuiter(table_name, intuit, descriptions=descriptions)

