"""Parsers and exractors for the bundle source files.

This module manages reading and writing files that configure a source bundle:

- bundle.py: Main code file for building a bundle
- meta.py: One-time executed code for manipulating bundle metadata.
- bundle.yaml: Main metadata file.
- schema.csv: Describes tables and columns.
- column_map.csv: Maps column names from a source file to the schema
- sources.csv: Describes the name, description and URL of input data

This module connects the filesystem to the File records in a dataset. A parallel module,
ambry.orm.files, connects between the File records and the other types of records in a Dataset

Build source file data is stored in File records in msgpack format. Files that are essentially spreadsheets,
such as schema, column_map and sources, are stored as a list of lists, one list per row.YAML files are stored as dicts,
and python files are stored as strings. Msgpack format is used because it is fast and small, which is important for
larget schema files, such as those in the US Census.

"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

from ambry.orm import File
import hashlib
import time
from ..util import Constant

class FileTypeError(Exception):
    """Bad file type"""

class BuildSourceFile(object):

    SYNC_DIR = Constant()
    SYNC_DIR.FILE_TO_RECORD = 'ftr'
    SYNC_DIR.RECORD_TO_FILE = 'rtf'

    def __init__(self, dataset, filesystem, file_const):
        """
        Construct a new Build Source File acessor
        :param dataset: The dataset that will hold File records
        :param filesystem: A FS filesystem abstraction object
        :param file_const: The BSFILE file contact
        :return:
        """

        assert not isinstance(filesystem, basestring) # Old Datatypes are leaking through.

        self._dataset = dataset
        self._fs = filesystem
        self._file_const = file_const

    def exists(self):
        return self._fs.exists(file_name(self._file_const))

    @property
    def record(self):
        return self._dataset.bsfile(self._file_const)

    @property
    def default(self):
        """Return default contents"""
        return file_default(self._file_const)

    def prepare_to_edit(self):
        """Ensure there is a file to edit, either by syncing to the filesystem or by installing the default"""

        if not self.record.contents and not self.exists():
            self._fs.setcontents(file_name(self._file_const), self.default)

        self.sync()

    @property
    def path(self):
        return self._fs.getsyspath(file_name(self._file_const))

    def fs_modtime(self):
        import time
        from fs.errors import ResourceNotFoundError

        fn_path = file_name(self._file_const)

        try:
            info = self._fs.getinfokeys(fn_path, "modified_time")
            return time.mktime(info['modified_time'].timetuple())
        except ResourceNotFoundError:
            return None

    def fs_hash(self):
        from ambry.util import md5_for_file

        if not self.exists():
            return None

        fn_path = file_name(self._file_const)
        with self._fs.open(fn_path) as f:
            return md5_for_file(f)

    def sync_dir(self):
        """ Report on which direction a synchronization should be done.
        :return:
        """

        if self.exists() and not self.record.size:
            # The fs exists, but the record is empty
            return self.SYNC_DIR.FILE_TO_RECORD

        elif self.record.size and not self.exists():
            # Record exists, but not the FS
            return self.SYNC_DIR.RECORD_TO_FILE

        if self.record.modified > self.fs_modtime():
            # Record is newer
            return self.SYNC_DIR.RECORD_TO_FILE

        elif self.fs_modtime() > self.record.modified:
            # Filesystem is newer
            return self.SYNC_DIR.FILE_TO_RECORD

        return None

    def sync(self, force = None):
        """Synchronize between the file in the file system and the fiel record"""

        from time import time

        if force:
            sd = force
        else:
            sd = self.sync_dir()

        if  sd == self.SYNC_DIR.FILE_TO_RECORD:

            if force and not self.exists():
                return None

            self.fs_to_record()

        elif sd == self.SYNC_DIR.RECORD_TO_FILE:
            self.record_to_fs()

        else:
            return None

        self._dataset.config.sync[self._file_const][sd] = time()
        return sd

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""
        raise NotImplementedError

    def record_to_fs(self):
        """Create a filesystem file from a File"""
        raise NotImplementedError


class RowBuildSourceFile(BuildSourceFile):
    """A Source Build file that is a list of rows, like a spreadsheet"""

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""
        import unicodecsv as csv

        import msgpack

        fn_path = file_name(self._file_const)

        fr = self._dataset.bsfile(self._file_const)
        fr.path = fn_path
        rows = []
        with self._fs.open(fn_path) as f:
            for row in csv.reader(f):
                row = [e if e.strip() != ''  else None for e in row]
                if any(bool(e) for e in row):
                    rows.append(row)

        fr.update_contents(msgpack.packb(rows))

        fr.mime_type = 'application/msgpack'
        fr.source_hash = self.fs_hash()

        fr.modified = self.fs_modtime()

    def record_to_fs(self):
        """Create a filesystem file from a File"""
        import unicodecsv as csv

        fr = self._dataset.bsfile(self._file_const)

        fn_path = file_name(self._file_const)

        if fr.contents:
            with self._fs.open(fn_path, 'wb') as f:
                w = csv.writer(f)
                for row in fr.unpacked_contents:
                    w.writerow(row)

class DictBuildSourceFile(BuildSourceFile):
    """A Source Build file that is a list of rows, like a spreadsheet"""

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""

        import msgpack

        fn_path = file_name(self._file_const)
        fr = self._dataset.bsfile(self._file_const)
        fr.path = fn_path
        if fn_path.endswith('.yaml'):
            import yaml

            with self._fs.open(fn_path) as f:
                fr.update_contents(msgpack.packb(yaml.load(f)))
            fr.mime_type = 'application/msgpack'
        else:
            raise FileTypeError("Unknown file type for : %s" % fn_path)

        fr.source_hash = self.fs_hash()

        fr.modified = self.fs_modtime()

    def record_to_fs(self):
        """Create a filesystem file from a File"""

        import msgpack
        import yaml

        fr = self._dataset.bsfile(self._file_const)

        fn_path = file_name(self._file_const)

        if fr.contents:
            with self._fs.open(fn_path, 'wb') as f:
                yaml.dump(fr.unpacked_contents, default_flow_style=False)


class StringSourceFile(BuildSourceFile):
    """A Source Build File that is a single file. """

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""

        fn_path = file_name(self._file_const)
        fr = self._dataset.bsfile(self._file_const)
        fr.path = fn_path

        with self._fs.open(fn_path) as f:
            fr.update_contents(unicode(f.read()))

        fr.mime_type = 'text/plain'
        fr.source_hash = self.fs_hash()
        fr.modified = self.fs_modtime()

    def record_to_fs(self):
        """Create a filesystem file from a File"""

        fr = self._dataset.bsfile(self._file_const)

        if fr.contents:
            with self._fs.open(file_name(self._file_const), 'wb') as f:
                f.write(fr.contents)

class MetadataFile(DictBuildSourceFile):

    def record_to_objects(self):
        """Create config records to match the file metadata"""
        from ..util import AttrDict

        fr = self._dataset.bsfile(self._file_const)

        contents = fr.unpacked_contents

        if not contents:
            return

        ad = AttrDict(contents)

        # Get time that filessystem was synchronized to the File record.
        # Maybe use this to avoid overwriting configs that changed by bundle program.
        # fs_sync_time = self._dataset.config.sync[self._file_const][self.file_to_record]

        self._dataset.config.metadata.set(ad)

        self._dataset._database.commit()

        return ad

    def objects_to_record(self):
        pass

class PythonSourceFile(StringSourceFile):

    def import_bundle_class(self):
        """Add the filesystem to the Python sys path with an import hook, then import
        to file as Python"""

        context = {}

        exec self._dataset.bsfile(self._file_const).contents in context

        return context['Bundle']

class SourcesFile(RowBuildSourceFile):

    def record_to_objects(self):
        """Create config records to match the file metadata"""
        from ..orm.source import DataSource

        fr = self._dataset.bsfile(self._file_const)

        contents = fr.unpacked_contents

        if not contents:
            return

        # Zip transposes an array when in the form of a list of lists, so this transposes so each row starts with the heading
        # and the rest of the row are the values for that row. The bool and filter return false when none of the values
        # are non-empty. Then zip again to transpose to original form.

        non_empty_rows = zip(*[ row for row in zip(*contents) if bool(filter(bool,row[1:])) ])

        s = self._dataset._database.session

        for i, row in enumerate(non_empty_rows):
            if i == 0:
                header = row
            else:
                d = dict(zip(header, row))

                if 'widths' in d:
                    del d['widths'] # Obsolete column in old spreadsheets.

                if 'table' in d:
                    d['dest_table_name'] = d['table']
                    del d['table']

                if 'dest_table' in d:
                    d['dest_table_name'] = d['dest_table']
                    del d['dest_table']

                if 'source_table' in d:
                    d['source_table_name'] = d['source_table']
                    del d['source_table']

                d['d_vid'] = self._dataset.vid

                ds = self._dataset.source_file(d['name'])
                if ds:
                    ds.update(**d)
                else:

                    ds = DataSource(**d)

                s.merge(ds)

            self._dataset._database.commit()

    def objects_to_record(self):
        pass

class SchemaFile(RowBuildSourceFile):

    def record_to_objects(self):
        """Create config records to match the file metadata"""

        from ..orm.source import DataSource
        from ..orm.file import File
        from ambry.dbexceptions import ConfigurationError
        import re

        def _clean_int(i):

            if i is None:
                return None
            elif isinstance(i, int):
                return i
            elif isinstance(i, basestring):
                if len(i) == 0:
                    return None

                return int(i.strip())

        bsfile = self._dataset.bsfile(self._file_const)

        contents = bsfile.unpacked_contents

        if not contents:
            return

        t = None

        new_table = True
        last_table = None
        line_no = 1  # Accounts for file header. Data starts on line 2

        errors = []
        warnings = []

        extant_tables = [t.name for t in self._dataset.tables]

        for row in bsfile.dict_row_reader:

            line_no += 1

            if not row.get('column', False) and not row.get('table', False):
                continue

            # Probably best not to have unicode in column names and descriptions.
            # row = {k: str(v).decode('utf8', 'ignore').encode('ascii', 'ignore').strip() for k, v in row.items()}

            if row['table'] and row['table'] != last_table:
                new_table = True
                last_table = row['table']

            if new_table and row['table']:

                if row['table'] in extant_tables:
                    errors.append((row['table'], None, "Table already exists"))
                    return warnings, errors

                try:
                    table_row = dict(**row)
                    del table_row['type']  # The field is really for columns, and means something different for tables

                    t = self._dataset.new_table(row['table'], **table_row)

                except Exception as e:
                    errors.append((None, None, " Failed to add table: {}. Row={}. Exception={}"
                                   .format(row['table'], dict(row), e)))
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

            data = {k.replace('d_', '', 1): v for k, v in row.items() if k.startswith('d_')}

            description = row.get('description', '').strip().encode('utf-8')

            col = t.add_column(row['column'],
                               is_primary_key=True if row.get('is_pk', False) else False,
                               fk_vid=row['is_fk'] if row.get('is_fk', False) else None, description=description,
                               datatype=datatype, proto_vid=row.get('proto_vid'), derivedfrom=row.get('derivedfrom'),
                               unique_constraints=','.join(uniques), indexes=','.join(indexes),
                               uindexes=','.join(uindexes),
                               default=default, size=size,  width=width, data=data,
                               sql=row.get('sql'),
                               scale=float(row['scale']) if row.get('scale', False) else None,
                               flags=row.get('flags', None),
                               keywords=row.get('keywords'), measure=row.get('measure'), units=row.get('units', None),
                               universe=row.get('universe'), commit=False)

            #if col:
            #    self.validate_column(t, col, warnings, errors)

        return warnings, errors


    def _dump_gen(self):
        """Yield schema row for use in exporting the schema to other
        formats

        """
        from collections import OrderedDict

        # Collect indexes
        indexes = {}

        # Sets the order of the fields
        all_opt_col_fields = ["size",  "default",  "width",
                              "description", "sql", "keywords",
                               "units", "universe", 'proto_vid', "derivedfrom"]

        # Collects what fields actually exist
        opt_fields_set = set()

        all_opt_table_fields = ["keywords", "universe"]

        data_fields = set()
        # Need to get all of the indexes figured out first, since there are a variable number of indexes.
        for table in self._dataset.tables:

            if table.proto_vid:
                opt_fields_set.add("proto_vid")

            for field in all_opt_table_fields:

                v = getattr(table, field)
                if v and field not in opt_fields_set:
                    opt_fields_set.add(field)

            for col in table.columns:

                if col.proto_vid:
                    opt_fields_set.add("proto_vid")

                for index_set in [col.indexes, col.uindexes]:
                    if not index_set:
                        continue  # HACK. This probably should not happen

                    for idx in index_set:

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

        for table in self._dataset.tables:

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
                    row['id'] = table.id
                    if table.proto_vid:
                        row['proto_vid'] = table.proto_vid

                    for field in all_opt_table_fields:

                        v = getattr(table, field)

                        if v and field in opt_fields_set:
                            row[field] = v

                else:
                    row['id'] = col.id
                    if col.proto_vid:
                        row['proto_vid'] = col.proto_vid

                yield row

    def objects_to_record(self):
        import msgpack

        rows = []

        last_table = None
        for row in self._dump_gen():

            if not rows:
                rows.append(row.keys())

            # Blank row to seperate tables.
            if last_table and row['table'] != last_table:
                rows.append([])

            rows.append(row.values())

            last_table = row['table']

        # Transpose tric to remove empty columns
        rows = zip(*[ row for row in zip(*rows) if bool(filter(bool,row[1:])) ])

        bsfile = self._dataset.bsfile(self._file_const)

        bsfile.mime_type = 'application/msgpack'
        bsfile.update_contents(msgpack.packb(rows))

        self._dataset._database.commit()

class SourceSchemaFile(RowBuildSourceFile):

    def record_to_objects(self):
        from ambry.dbexceptions import ConfigurationError
        bsfile = self._dataset.bsfile(self._file_const)

        failures = set()
        for row in bsfile.dict_row_reader:
            st = self._dataset.source_table(row['table'])

            if not st:
                st = self._dataset.new_source_table(row['table'])

            del row['table']
            st.add_column(**row) # Create or update

        if failures:
            raise ConfigurationError("Failed to load source schema, missing sources: {} ".format(failures))

    def objects_to_record(self):

        import msgpack
        bsfile = self._dataset.bsfile(self._file_const)

        rows = []
        for table in self._dataset.source_tables:

            for column in table.columns:
                row = column.row
                if not rows:
                    rows.append(row.keys())

                rows.append(row.values())

        bsfile.mime_type = 'application/msgpack'
        bsfile.update_contents(msgpack.packb(rows))

        self._dataset._database.commit()

class PartitionsFile(RowBuildSourceFile):

    def record_to_objects(self):
        from ambry.dbexceptions import ConfigurationError
        bsfile = self._dataset.bsfile(self._file_const)

        failures = set()
        for row in bsfile.dict_row_reader:
            self._dataset.new_partition(**row) # Create or update


    def objects_to_record(self):

        import msgpack
        bsfile = self._dataset.bsfile(self._file_const)

        rows = []
        for table in self._dataset.partitions:
            for column in table.columns:
                row = column.row
                if not rows:
                    rows.append(row.keys())

                rows.append(row.values())

        bsfile.mime_type = 'application/msgpack'
        bsfile.update_contents(msgpack.packb(rows))

        self._dataset._database.commit()


file_info_map = {
    File.BSFILE.BUILD : ('bundle.py',PythonSourceFile),
    File.BSFILE.BUILDMETA: ('meta.py',PythonSourceFile),
    File.BSFILE.DOC: ('documentation.md',StringSourceFile),
    File.BSFILE.META: ('bundle.yaml',MetadataFile),
    File.BSFILE.SCHEMA: ('schema.csv',SchemaFile),
    File.BSFILE.SOURCESCHEMA: ('source_schema.csv', SourceSchemaFile),
    File.BSFILE.SOURCES: ('sources.csv',SourcesFile),
    File.BSFILE.PARTITIONS: ('partitions.csv', PartitionsFile)
}

def file_name(const):
    """Return the file name for a file constant"""
    return file_info_map[const][0]

def file_class(const):
    """Return the class for a file constant"""
    return file_info_map[const][1]

def file_default(const):
    """Return the default content for the file"""

    import ambry.bundle.default_files as df
    import os

    path = os.path.join(os.path.dirname(df.__file__),  file_name(const))

    with open(path) as f:
        return f.read()




class BuildSourceFileAccessor(object):

    def __init__(self, dataset, filesystem = None):
        assert not isinstance(filesystem, basestring ) # Bundle fs changed from FS to URL; catch use of old values
        self._dataset = dataset
        self._fs = filesystem

    @property
    def build_file(self):
        return self.file(File.BSFILE.BUILD)

    @property
    def meta_file(self):
        return self.file(File.BSFILE.META)

    def file(self, const_name):

        fc = file_class(const_name)

        bsfile = fc(self._dataset, self._fs, const_name)

        return bsfile

    def sync(self, force = None, defaults = False):

        syncs = []

        for file_const, (file_name, clz) in  file_info_map.items():
            f = self.file(file_const)

            if defaults and force == f.SYNC_DIR.RECORD_TO_FILE and  not f.record.contents:
                syncs.append((file_const, f.prepare_to_edit()))
            else:
                syncs.append((file_const,f.sync(force)))

        return syncs

    def sync_dirs(self):
        return [ (file_const, self.file(file_const).sync_dir() )
                 for file_const, (file_name, clz) in  file_info_map.items() ]




