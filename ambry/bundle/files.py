# -*- coding: utf-8 -*-

"""Parsers and exractors for the bundle source files.

This module manages reading and writing files that configure a source bundle:

- bundle.py: Main code file for building a bundle.
- bundle.sql: Set of SQL statements. The system loads the SQL file and creates all of the tables,
    so they can be referenced during the build.
- bundle.yaml: Main metadata file.
- column_map.csv: Maps column names from a source file to the schema.
- meta.py: One-time executed code for manipulating bundle metadata.
- schema.csv: Describes tables and columns.
- sources.csv: Describes the name, description and URL of input data.

This module connects the filesystem to the File records in a dataset. A parallel module,
ambry.orm.files, connects between the File records and the other types of records in a Dataset

Build source file data is stored in File records in msgpack format. Files that are essentially spreadsheets,
such as schema, column_map and sources, are stored as a list of lists, one list per row. YAML files
are stored as dicts, and python and sql files are stored as strings. Msgpack format is used because it is
fast and small, which is important for largest schema files, such as those in the US Census.

"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

import imp
import msgpack
import sys
import time
import yaml

import csv

import six
from six import string_types, iteritems, u, iterkeys

from ambry.dbexceptions import ConfigurationError
from ambry.orm import File
from ambry.util import Constant, get_logger, drop_empty

logger = get_logger(__name__)


class FileTypeError(Exception):
    """Bad file type"""


class BuildSourceFile(object):

    SYNC_DIR = Constant()
    SYNC_DIR.FILE_TO_RECORD = 'ftr'
    SYNC_DIR.RECORD_TO_FILE = 'rtf'
    SYNC_DIR.OBJECT_TO_FILE = 'otf'

    multiplicity = '1'  # Number of files of this type. '1' or 'n'
    file_const = None

    def __init__(self, bundle, dataset, filesystem,  base_name=None):
        """
        Construct a new Build Source File acessor
        :param dataset: The dataset that will hold File records
        :param filesystem: A FS filesystem abstraction object
        :param file_const: The BSFILE file contact
        :return:
        """

        assert not isinstance(filesystem, string_types)  # Old Datatypes are leaking through.

        self._bundle = bundle
        self._dataset = dataset
        self._fs = filesystem

        assert self.multiplicity == '1' or base_name, 'File class {} requires a base_name'.format(type(self))

        self._base_name = base_name  # The root of the file name

    @classmethod
    def instance_from_filename(cls, name, bundle, dataset, fs):

        import re

        if cls.multiplicity == '1' and name == cls._file_name:
            return cls(bundle, dataset, fs)

        elif re.match('^'+cls._file_name.format(base_name='.*')+'$', name):
            # HACK Assume that the base name is everything left of the extension, which may not
            # always be true in the future
            base_name = name.split('.')[0]

            return cls(bundle, dataset, fs, base_name)

        return None

    def exists(self):
        if self.multiplicity == '1':
            return self._fs.exists(self.file_name)
        else:
            assert self._base_name, 'File class {} requires a base_name'.format(type(self))
            return self._fs.exists(self.file_name)

    def size(self):
        return self._fs.getsize(self.file_name)

    @property
    def record_exists(self):
        from ambry.orm.exc import NotFoundError
        try:
            _ = self._dataset.bsfile(self.file_const, self.file_name)
            return True
        except NotFoundError:
            return False

    @property
    def record(self):
        return self._dataset.find_or_new_bsfile(self.file_const, self.file_name)

    @property
    def extant_record(self):
        """Only return a record that actually exists"""
        return self._dataset.bsfile(self.file_name)

    @property
    def record_content(self):
        """Return the contents of the file record"""
        return self.record.unpacked_contents

    @property
    def file_content(self):
        """Return the contents of the system file"""
        return self._fs.getcontents(self.file_name)

    @property
    def default(self):
        """Return default contents"""

        import ambry.bundle.default_files as df
        import os

        path = os.path.join(os.path.dirname(df.__file__), self.file_name)

        if six.PY2:
            with open(path, 'rb') as f:
                return f.read()
        else:
            # py3
            with open(path, 'rt', encoding='utf-8') as f:
                return f.read()

    def set_defaults(self):
        self.setcontent(self.default)

    @property
    def path(self):
        """ Returns system path of the file. """
        return self._fs.getsyspath(self.file_name)

    def remove(self):
        """ Removes file from filesystem. """
        from fs.errors import ResourceNotFoundError

        try:
            self._fs.remove(self.file_name)
        except ResourceNotFoundError:
            pass

    def remove_record(self):
        self._bundle.session.delete(self.extant_record)

    @property
    def file_name(self):
        if self.multiplicity == '1':
            return self._file_name
        else:
            assert self._base_name is not None
            return self._file_name.format(base_name=self._base_name)

    @property
    def fs_modtime(self):
        from fs.errors import ResourceNotFoundError

        fn_path = self.file_name

        try:
            info = self._fs.getinfokeys(fn_path, "modified_time")
            return time.mktime(info['modified_time'].timetuple())
        except ResourceNotFoundError:
            return None

    @property
    def fs_is_newer(self):

        return ((self.fs_modtime or 0) > (self.record.modified or 0)
                and self.record.source_hash != self.fs_hash)

    @property
    def fs_mod_since_sync(self):

        return ((self.fs_modtime or 0) > (self.record.synced_fs or 0))

    @property
    def same_age(self):
        return (self.fs_modtime or 0) == (self.record.modified or 0)

    @property
    def fs_hash(self):
        from ambry.util import md5_for_file

        if not self.exists():
            return None

        fn_path = self.file_name

        with self._fs.open(fn_path, mode='rb') as f:
            return md5_for_file(f)

    def sync_dir(self):
        """ Report on which direction a synchronization should be done.
        :return:
        """

        # NOTE: These are ordered so the FILE_TO_RECORD has preference over RECORD_TO_FILE
        # if there is a conflict.

        if self.exists() and bool(self.size()) and not self.record.size:
            # The fs exists, but the record is empty
            return self.SYNC_DIR.FILE_TO_RECORD

        if (self.fs_modtime or 0) > (self.record.modified or 0) and self.record.source_hash != self.fs_hash:
            # Filesystem is newer

            return self.SYNC_DIR.FILE_TO_RECORD

        if self.record.size and not self.exists():
            # Record exists, but not the FS

            return self.SYNC_DIR.RECORD_TO_FILE

        if (self.record.modified or 0) > (self.fs_modtime or 0):
            # Record is newer
            return self.SYNC_DIR.RECORD_TO_FILE

        return None

    def sync(self, force=None):
        """Synchronize between the file in the file system and the field record"""

        try:
            if force:
                sd = force
            else:
                sd = self.sync_dir()

            if sd == self.SYNC_DIR.FILE_TO_RECORD:

                if force and not self.exists():
                    return None

                self.fs_to_record()

            elif sd == self.SYNC_DIR.RECORD_TO_FILE:
                self.record_to_fs()

            else:
                return None

            self._dataset.config.sync[self.file_const][sd] = time.time()
            return sd
        except Exception as e:
            self._bundle.rollback()
            self._bundle.error("Failed to sync '{}': {}".format(self.file_const, e))
            raise

    def sync_out(self):
        """Sync out from objects to the file system"""



        self.objects_to_record()
        self.record_to_fs()

    def clean_objects(self):
        pass

    def fh_to_record(self, f):
        raise NotImplementedError

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""
        raise NotImplementedError

    def record_to_fh(self, f):
        """Create a filesystem file from a File"""
        raise NotImplementedError

    def record_to_fs(self):
        """Create a filesystem file from a File"""
        raise NotImplementedError

    def record_to_objects(self):
        """Create objects from the file record"""
        raise NotImplementedError

    def fs_to_objects(self):
        """Load the file from the file system, to the record, and then to the objects"""
        self.fs_to_record()
        self.record_to_objects()

    def setcontent(self, content):

        from cStringIO import StringIO

        return self.fh_to_record(StringIO(content))

    def getcontent(self):
        from cStringIO import StringIO

        sio = StringIO()

        self.record_to_fh(sio)

        return sio.getvalue()

    def list_files(self):
        """Return a list of the file name for this file type. Usuall, this will be just one,
        ( multiplicity == '1' ) but may be more when multiplicity = 'n' )"""

    def list_records(self):
        """Like list_files, but for records"""


class RowBuildSourceFile(BuildSourceFile):
    """A Source Build file that is a list of rows, like a spreadsheet"""

    def fs_to_record(self):

        fn_path = self.file_name

        if six.PY2:
            with self._fs.open(fn_path, 'rb') as f:
                return self.fh_to_record(f)
        else:

            with self._fs.open(fn_path, 'rt', encoding='utf-8') as f:
                return self.fh_to_record(f)

    def fh_to_record(self, f):
        """Load a file in the filesystem into the file record"""
        import unicodecsv as csv

        fn_path = self.file_name

        fr = self.record
        fr.path = fn_path
        rows = []

        # NOTE. There were two cases here, for PY2 and PY3. Py two had
        # encoding='utf-8' in the reader. I've combined them b/c that's the default for
        # unicode csv, so it shouldn't be necessary.

        # Should probably be something like this:
        #if sys.version_info[0] >= 3:  # Python 3
        #    import csv
        #    f = open(self._fstor.syspath, 'rtU', encoding=encoding)
        #    reader = csv.reader(f)
        #else:  # Python 2
        #    import unicodecsv as csv
        #    f = open(self._fstor.syspath, 'rbU')
        #    reader = csv.reader(f, encoding=encoding)

        for row in csv.reader(f):
            row = [e if e.strip() != '' else None for e in row]
            if any(bool(e) for e in row):
                rows.append(row)
        try:
            fr.update_contents(msgpack.packb(rows), 'application/msgpack')
        except AssertionError:
            raise

        fr.source_hash = self.fs_hash
        fr.synced_fs = self.fs_modtime
        fr.modified = self.fs_modtime

    def record_to_fh(self, f):
        import unicodecsv as csv

        fr = self.record

        # Some types have special representations in spreadsheets, particularly lists and dicts
        def munge_types(v):
            if isinstance(v, (list, tuple)):
                return u(',').join(u('{}').format(e).replace(',', '\,') for e in v)
            elif isinstance(v, dict):
                import json
                return json.dumps(v)
            else:
                return v

        if fr.contents:

            w = csv.writer(f, encoding='utf-8')
            for i, row in enumerate(fr.unpacked_contents):
                w.writerow([munge_types(e) for e in row])

            fr.source_hash = self.fs_hash
            fr.modified = self.fs_modtime

    def record_to_fs(self):
        """Create a filesystem file from a File"""

        fr = self.record

        fn_path = self.file_name

        if fr.contents:
            if six.PY2:
                with self._fs.open(fn_path, 'wb') as f:
                    self.record_to_fh(f)
            else:
                # py3
                with self._fs.open(fn_path, 'w', newline='') as f:
                    self.record_to_fh(f)


class DictBuildSourceFile(BuildSourceFile):
    """A Source Build file that is a list of rows, like a spreadsheet"""

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""
        fn_path = self.file_name

        with self._fs.open(fn_path, mode='r', encoding='utf-8') as f:
            return self.fh_to_record(f)

    def fh_to_record(self, f):
        from ambry.util import md5_for_stream

        fn_path = self.file_name
        fr = self.record
        fr.path = fn_path

        self.set_dict(yaml.safe_load(f))

        fr.source_hash = self.fs_hash # FIXME! Assumes a file system entry, but may only be a file handle!

        fr.synced_fs = self.fs_modtime
        fr.modified = self.fs_modtime



    def set_dict(self, d):
        self.record.update_contents(msgpack.packb(d), 'application/msgpack')


    def record_to_fh(self, f):
        """Write the record, in filesystem format, to a file handle or file object"""

        fr = self.record

        if fr.contents:
            yaml.safe_dump(fr.unpacked_contents, f, default_flow_style=False, encoding='utf-8')
            fr.source_hash = self.fs_hash
            fr.modified = self.fs_modtime

    def record_to_fs(self):
        """Create a filesystem file from a File"""

        fr = self.record

        fn_path = self.file_name

        if fr.contents:
            with self._fs.open(fn_path, 'w', encoding='utf-8') as f:

                self.record_to_fh(f)


class StringSourceFile(BuildSourceFile):
    """A Source Build File that is a single file. """

    def clean_objects(self):
        """This sort of file can only be set from files, and there are no associated object"""
        pass

    def record_to_objects(self):
        pass

    def objects_to_record(self):
        pass

    def fs_to_record(self):

        fn_path = self.file_name

        with self._fs.open(fn_path, 'r', encoding='utf-8') as f:
            return self.fh_to_record(f)

    def fh_to_record(self, f):
        """Load a file in the filesystem into the file record"""

        fn_path = self.file_name
        fr = self.record
        fr.path = fn_path

        fr.update_contents(f.read(), 'text/plain')

        fr.source_hash = self.fs_hash
        fr.synced_fs = self.fs_modtime
        fr.modified = self.fs_modtime

    def record_to_fh(self, f):
        fr = self.record

        if fr.contents:
            f.write(fr.unpacked_contents)
            fr.source_hash = self.fs_hash

            if self.fs_modtime:
                fr.modified = self.fs_modtime

    def record_to_fs(self):
        """Create a filesystem file from a File"""

        fr = self.record

        if fr.contents:
            # No UTF-Encoding! Just go directly from the the database to the file.
            with self._fs.open(self.file_name, 'wb') as f:
                self.record_to_fh(f)


class DocSourceFile(StringSourceFile):
    file_const = File.BSFILE.DOC
    _file_name = 'documentation.md'


class MetadataFile(DictBuildSourceFile):
    file_const = File.BSFILE.META
    _file_name = 'bundle.yaml'

    def clean_objects(self):
        self._dataset.configs = [c for c in self._dataset.configs if c.type != 'metadata']

        # TODO: Not sure if these should be cleaned or no

    def record_to_objects(self):
        """Create config records to match the file metadata"""
        from ..util import AttrDict

        fr = self.record

        contents = fr.unpacked_contents

        if not contents:
            return

        ad = AttrDict(contents)


        # Get time that filessystem was synchronized to the File record.
        # Maybe use this to avoid overwriting configs that changed by bundle program.
        # fs_sync_time = self._dataset.config.sync[self.file_const][self.file_to_record]

        self._dataset.config.metadata.set(ad)

        self._dataset._database.commit()

        return ad

    def objects_to_record(self):
        """Write from object metadata to the record. Note that we don't write everything"""

        o = self.get_object()

        o.about = self._bundle.metadata.about
        o.identity = self._dataset.identity.ident_dict
        o.names = self._dataset.identity.names_dict
        o.contacts = self._bundle.metadata.contacts

        self.set_object(o)


    def record_to_fh(self, f):

        fr = self.record

        if fr.has_contents:

            o = fr.unpacked_contents

        else:
            o = yaml.safe_load(self.default)

            try:
                act = self._bundle.library.config.accounts.ambry.to_dict()

                if act:
                    o['contacts']['wrangler'] = act

            except ConfigurationError:
                pass

            o['identity'] = self._dataset.identity.ident_dict
            o['names'] = self._dataset.identity.names_dict

        with self._fs.open(self.file_name, 'w', encoding='utf-8') as f:
            yaml.safe_dump(o, f, default_flow_style=False, indent=4, encoding='utf-8')

        return fr

    def update_identity(self):
        """Update the identity and names to match the dataset id and version"""

        fr = self.record

        d = fr.unpacked_contents

        d['identity'] = self._dataset.identity.ident_dict
        d['names'] = self._dataset.identity.names_dict

        fr.update_contents(msgpack.packb(d), 'application/msgpack')


    def get_object(self):
        """Return contents in object form, an AttrDict"""

        from ..util import AttrDict

        c = self.record.unpacked_contents

        if not c:
            c = yaml.safe_load(self.default)

        return AttrDict(c)

    def set_object(self, o):
        """Set contents as an AttrDict"""

        self.set_dict(o.to_dict())


class NotebookFile(StringSourceFile):

    multiplicity = 'n'
    file_const = File.BSFILE.NOTEBOOK
    _file_name = '{base_name}.ipynb'

    @property
    def default(self):
        """Return default contents"""
        import json

        import ambry.bundle.default_files as df
        import os

        path = os.path.join(os.path.dirname(df.__file__), 'notebook.ipynb')

        if six.PY2:
            with open(path, 'rb') as f:
                content_str = f.read()
        else:
            # py3
            with open(path, 'rt', encoding='utf-8') as f:
                content_str = f.read()

        c = json.loads(content_str)

        context = {
            'title': self._bundle.metadata.about.title,
            'summary': self._bundle.metadata.about.title,
            'bundle_vname': self._bundle.identity.vname
        }

        for cell in c['cells']:
            for i in range(len(cell['source'])):
                cell['source'][i] = cell['source'][i].format(**context)

        c['metadata']['ambry'] = {
            'identity': self._bundle.identity.dict
        }

        return json.dumps(c, indent=4)

    def execute(self):
        """Convert the notebook to a python script and execute it, returning the local context
        as a dict"""

        from nbformat import read
        from nbconvert.exporters import export_script
        from cStringIO import StringIO

        notebook = read(StringIO(self.record.unpacked_contents), 4)

        script, resources = export_script(notebook)

        env_dict = {}

        exec (compile(script.replace('# coding: utf-8', ''), 'script', 'exec'), env_dict)

        return env_dict


class PythonSourceFile(StringSourceFile):

    def clean_objects(self):
        """The python sources can only be set from files, and there are no associated objects"""
        pass

    def record_to_fs(self):
        """Create a filesystem file from a File"""

        fr = self.record

        if fr.contents:
            with self._fs.open(self.file_name, 'w', encoding='utf-8') as f:
                self.record_to_fh(f)

    def import_module(self, **kwargs):
        """
        Import the contents of the file into the ambry.build module

        :param kwargs: items to add to the module globals
        :return:
        """
        from fs.errors import NoSysPathError

        try:
            import ambry.build
            module = sys.modules['ambry.build']
        except ImportError:
            module = imp.new_module('ambry.build')
            sys.modules['ambry.build'] = module

        bf = self.record

        if not bf.contents:
            return module

        module.__dict__.update(**kwargs)

        try:
            abs_path = self._fs.getsyspath(self.file_name)
        except NoSysPathError:
            abs_path = '<string>'

        import re

        if re.search(r'-\*-\s+coding:', bf.contents):
            # Has encoding, so don't decode
            contents = bf.contents
        else:
            contents = bf.unpacked_contents  # Assumes utf-8

        exec(compile(contents, abs_path, 'exec'), module.__dict__)

        return module

    def import_bundle(self):
        """Add the filesystem to the Python sys path with an import hook, then import
        to file as Python"""
        from fs.errors import NoSysPathError

        try:
            import ambry.build
            module = sys.modules['ambry.build']
        except ImportError:
            module = imp.new_module('ambry.build')
            sys.modules['ambry.build'] = module

        bf = self.record

        if not bf.has_contents:
            from ambry.bundle import Bundle
            return Bundle

        try:
            abs_path = self._fs.getsyspath(self.file_name)
        except NoSysPathError:
            abs_path = '<string>'

        exec(compile(bf.contents, abs_path, 'exec'), module.__dict__)

        return module.Bundle

    def import_lib(self):
        """Import the lib.py file into the bundle module"""

        try:
            import ambry.build
            module = sys.modules['ambry.build']
        except ImportError:
            module = imp.new_module('ambry.build')
            sys.modules['ambry.build'] = module

        bf = self.record

        if not bf.has_contents:
            return

        try:
            exec (compile(bf.contents, self.path, 'exec'), module.__dict__)

        except Exception:
            self._bundle.error("Failed to load code from {}".format(self.path))
            raise

        # print(self.file_const, bundle.__dict__.keys())
        # print(bf.contents)

        return module


class BuildPythonSourceFile(PythonSourceFile):

    file_const = File.BSFILE.BUILD
    _file_name = 'bundle.py'


class LibPythonSourceFile(PythonSourceFile):
    file_const = File.BSFILE.LIB
    _file_name = 'lib.py'


class TestPythonSourceFile(PythonSourceFile):
    file_const = File.BSFILE.TEST
    _file_name = 'test.py'


class SourcesFile(RowBuildSourceFile):

    file_const = File.BSFILE.SOURCES
    _file_name = 'sources.csv'

    def clean_objects(self):

        self._dataset.sources[:] = []

    def record_to_objects(self):
        """Create config records to match the file metadata"""
        from ambry.orm.exc import NotFoundError

        fr = self.record

        contents = fr.unpacked_contents

        if not contents:
            return

        # Zip transposes an array when in the form of a list of lists, so this transposes so
        # each row starts with the heading and the rest of the row are the values
        # for that row. The bool and filter return false when none of the values
        # are non-empty. Then zip again to transpose to original form.

        non_empty_rows = drop_empty(contents)

        s = self._dataset._database.session

        for i, row in enumerate(non_empty_rows):

            if i == 0:
                header = row
            else:
                d = dict(six.moves.zip(header, row))

                if 'widths' in d:
                    del d['widths']  # Obsolete column in old spreadsheets.

                if 'table' in d:
                    d['dest_table_name'] = d['table']
                    del d['table']

                if 'order' in d:
                    d['stage'] = d['order']
                    del d['order']

                if 'dest_table' in d:
                    d['dest_table_name'] = d['dest_table']
                    del d['dest_table']

                if 'source_table' in d:
                    d['source_table_name'] = d['source_table']
                    del d['source_table']

                d['d_vid'] = self._dataset.vid

                d['state'] = 'synced'

                try:
                    ds = self._dataset.source_file(str(d['name']))
                    ds.update(**d)
                except NotFoundError:
                    name = d['name']
                    del d['name']

                    ds = self._dataset.new_source(name, **d)
                except:  # Odd error with 'none' in keys for d
                    print('!!!', header)
                    print('!!!', row)
                    raise

                s.merge(ds)

        self._dataset._database.commit()

    def objects_to_record(self):

        sorter = lambda r: ('A' if r['reftype'] == 'ref'
                            else 'z' if r['reftype'] is None
                            else r['reftype'], r['name'])

        rows = sorted([s.row for s in self._dataset.sources], key=sorter)

        if rows:
            rows = [list(rows[0].keys())] + [list(r.values()) for r in rows]

            # Transpose trick to remove empty columns
            rows = list(drop_empty(rows))
        else:
            # No contents, so use the default file
            rows = list(csv.reader(self.default.splitlines()))

        bsfile = self.record

        bsfile.update_contents(msgpack.packb(rows), 'application/msgpack')


class SchemaFile(RowBuildSourceFile):

    file_const = File.BSFILE.SCHEMA
    _file_name = 'schema.csv'

    def clean_objects(self):
        self._dataset.tables[:] = []

    def record_to_objects(self):
        """Create config records to match the file metadata"""
        from ambry.orm import Column, Table, Dataset

        def _clean_int(i):
            if i is None:
                return None
            elif isinstance(i, int):
                return i
            elif isinstance(i, string_types):
                if len(i) == 0:
                    return None

                return int(i.strip())

        bsfile = self.record

        contents = bsfile.unpacked_contents

        if not contents:
            return

        line_no = 1  # Accounts for file header. Data starts on line 2

        errors = []
        warnings = []

        extant_tables = {t.name: t for t in self._dataset.tables}

        old_types_map = {
            'varchar': Column.DATATYPE_STR,
            'integer': Column.DATATYPE_INTEGER,
            'real': Column.DATATYPE_FLOAT,
        }

        def run_progress_f(line_no):
            self._bundle.log('Loading tables from file. Line #{}'.format(line_no))

        from ambry.bundle.process import CallInterval
        run_progress_f = CallInterval(run_progress_f, 10)

        table_number = self._dataset._database.next_sequence_id(Dataset, self._dataset.vid, Table)
        for row in bsfile.dict_row_reader:

            line_no += 1

            run_progress_f(line_no)

            # Skip blank lines
            if not row.get('column', False) and not row.get('table', False):
                continue

            if not row.get('column', False):
                raise ConfigurationError('Row error: no column on line {}'.format(line_no))
            if not row.get('table', False):
                raise ConfigurationError('Row error: no table on line {}'.format(line_no))
            if not row.get('datatype', False):
                raise ConfigurationError('Row error: no type on line {}'.format(line_no))

            row['datatype'] = old_types_map.get(row['datatype'].lower(), row['datatype'])

            table_name = row['table']

            try:
                table = extant_tables[table_name]
            except KeyError:
                table = self._dataset.new_table(
                    table_name,
                    sequence_id=table_number,
                    description=row.get('description') if row['column'] == 'id' else ''
                )

                table_number += 1
                extant_tables[table_name] = table

            data = {k.replace('d_', '', 1): v
                    for k, v in list(row.items()) if k and k.startswith('d_') and v}

            if row['column'] == 'id':
                table.data.update(data)
                data = {}

            table.add_column(
                row['column'],
                fk_vid=row['is_fk'] if row.get('is_fk', False) else None,
                description=(row.get('description', '') or '').strip(),
                datatype=row['datatype'].strip().lower() if '.' not in row['datatype'] else row['datatype'],
                proto_vid=row.get('proto_vid'),
                size=_clean_int(row.get('size', None)),
                width=_clean_int(row.get('width', None)),
                data=data,
                keywords=row.get('keywords'),
                measure=row.get('measure'),
                transform=row.get('transform'),
                derivedfrom=row.get('derivedfrom'),
                units=row.get('units', None),
                universe=row.get('universe'),
                update_existing= True)

        self._dataset.t_sequence_id = table_number

        return warnings, errors

    def objects_to_record(self):

        initial_rows = []

        headers = []

        for table in self._dataset.tables:
            for col in table.columns:
                row = col.row
                initial_rows.append(row)

                # this should put all of the data fields at the end of the headers
                for k in iterkeys(row):
                    if k not in headers:
                        headers.append(k)

        rows = list()

        # Move description to the end
        if 'description' in headers:
            headers.remove('description')
            headers.append('description')

        if initial_rows:
            rows.append(headers)
            name_index = headers.index('column')
        else:
            name_index = None

        for row in initial_rows:

            this_row = list()
            for h in headers:  # Every row is the same length, with combined set of headers
                this_row.append(row.get(h, None))

            if name_index and this_row[name_index] == 'id':
                # Blank to separate tables, but transpose trick fails if rows not all same size
                rows.append([None for e in this_row])

            rows.append(this_row)

        # Transpose trick to remove empty columns
        if rows:
            rows_before_transpose = len(rows)
            rows = list(drop_empty(rows))
            assert rows_before_transpose == len(rows)  # The transpose trick removes all of the rows if anything goes wrong

        else:
            # No contents, so use the default file
            rows = list(csv.reader(self.default.splitlines()))

        bsfile = self.record
        bsfile.update_contents(msgpack.packb(rows), 'application/msgpack')


class SourceSchemaFile(RowBuildSourceFile):

    file_const = File.BSFILE.SOURCESCHEMA
    _file_name = 'source_schema.csv'

    def clean_objects(self):
        self._dataset.source_tables[:] = []

    def record_to_objects(self):
        """Write from the stored file data to the source records"""
        from ambry.orm import SourceTable

        bsfile = self.record

        failures = set()

        # Clear out all of the columns from existing tables. We don't clear out the
        # tables, since they may be referenced by sources

        for row in bsfile.dict_row_reader:
            st = self._dataset.source_table(row['table'])

            if st:
                st.columns[:] = []

        self._dataset.commit()

        for row in bsfile.dict_row_reader:
            st = self._dataset.source_table(row['table'])

            if not st:
                st = self._dataset.new_source_table(row['table'])
                # table_number += 1

            if 'datatype' not in row:
                row['datatype'] = 'unknown'

            del row['table']

            st.add_column(**row)  # Create or update

        if failures:
            raise ConfigurationError('Failed to load source schema, missing sources: {} '.format(failures))

        self._dataset.commit()

    def objects_to_record(self):

        bsfile = self.record

        rows = []
        for table in self._dataset.source_tables:

            for column in table.columns:
                rows.append(column.row)

        if rows:
            rows = [list(rows[0].keys())] + [list(r.values()) for r in rows]

        else:
            # No contents, so use the default file
            rows = list(csv.reader(self.default.splitlines()))

        bsfile.update_contents(msgpack.packb(rows), 'application/msgpack')


class ASQLSourceFile(StringSourceFile):
    """ Stores bundle.sql file content. """

    multiplicity = 'n'
    file_const = File.BSFILE.SQL
    _file_name = '{base_name}.sql'

    def parsed(self):

        from ambry.mprlib import parse_sql

        return parse_sql(self._bundle.library, self.record_content)

    def execute(self):
        """ Executes all sql statements from bundle.sql. """
        from ambry.mprlib import execute_sql

        execute_sql(self._bundle.library, self.record_content)

file_classes = {
    File.BSFILE.BUILD: BuildPythonSourceFile,
    File.BSFILE.LIB: LibPythonSourceFile,
    File.BSFILE.TEST: TestPythonSourceFile,
    File.BSFILE.DOC: DocSourceFile,
    File.BSFILE.META: MetadataFile,
    File.BSFILE.SCHEMA: SchemaFile,
    File.BSFILE.SOURCESCHEMA: SourceSchemaFile,
    File.BSFILE.SOURCES: SourcesFile,
    File.BSFILE.SQL: ASQLSourceFile,
    File.BSFILE.NOTEBOOK: NotebookFile
}

file_info_map = None # FIXME. Replacing this. Remove on sight


def file_class(const):
    """Return the class for a file constant"""
    return file_info_map[const][1]


class BuildSourceFileAccessor(object):

    def __init__(self, bundle, dataset, filesystem=None):
        assert not isinstance(filesystem, string_types)  # Bundle fs changed from FS to URL; catch use of old values
        self._bundle = bundle
        self._dataset = dataset
        self._fs = filesystem

    @property
    def build_file(self):
        raise NotImplementedError('Use self.build_bundle')
        return self.file(File.BSFILE.BUILD)

    def __getattr__(self, item):
        """Converts the file_constants into acessor names to return bsfiles.

        See File.BSFILE for the const string values. Returns a file via the self.file() method

        """

        if item in file_classes:
            return self.file(item)
        else:
            raise AttributeError(item)

    def __getitem__(self, item):
        """Converts the file_constants into acessor names to return bsfiles.

        See File.BSFILE for the const string values. Returns a file via the self.file() method

        """
        from sqlalchemy.orm.exc import NoResultFound

        try:
            f = self.file_by_path(item)
            return f
        except NoResultFound:
            pass

        if item in file_classes:
            return self.file(item)

        raise KeyError(item)

    def __iter__(self):
        """Iterate through all of the files, both records and files in filesystem"""

        seen = set()

        for f in self.list_records():
            if f.path not in seen:
                seen.add(f.path)
                yield f

        for f in self.list_records():
            if f.path not in seen:
                seen.add(f.path)
                yield f

    def list_files(self):
        """Iterate through the files in the filesystem"""
        for f in self._fs.listdir():
            yield self.instance_from_name(f)

    def list_records(self, file_const=None):
        """Iterate through the file records"""
        for r in self._dataset.files:
            if file_const and r.minor_type != file_const:
                continue
            yield self.instance_from_name(r.path)

    @property
    def meta_file(self):
        return self.file(File.BSFILE.META)

    def instance_from_name(self, name):

        for c in file_classes.values():
            o = c.instance_from_filename(name, self._bundle, self._dataset, self._fs)

            if o:
                return o

        return None

    def file(self, const_name, base_name=None):

        fc = file_classes[const_name]

        bsfile = fc(self._bundle, self._dataset, self._fs, base_name)

        return bsfile

    def file_by_path(self, path):

        s = self._bundle.session

        return s.query(File) \
            .filter(File.path == path) \
            .filter(File.d_vid == self._dataset.vid) \
            .first()

    def file_by_id(self, id):
        s = self._dataset._database.session

        return s.query(File).filter(File.d_vid == self._dataset.vid).filter(File.id == id).one()


    def record_to_objects(self, preference=None):
        """Create objects from files, or merge the files into the objects. """
        from ambry.orm.file import File

        for f in self.list_records():

            pref = preference if preference else f.record.preference

            if pref == File.PREFERENCE.FILE:
                self._bundle.logger.debug('   Cleaning objects for file {}'.format(f.path))
                f.clean_objects()

            if pref in (File.PREFERENCE.FILE, File.PREFERENCE.MERGE):
                self._bundle.logger.debug('   rto {}'.format(f.path))
                f.record_to_objects()

    def objects_to_record(self, preference=None):
        """Create file records from objects. """
        from ambry.orm.file import File

        raise NotImplementedError("Still uses obsolete file_info_map")
        for file_const, (file_name, clz) in iteritems(file_info_map):
            f = self.file(file_const)

            pref = preference if preference else f.record.preference

            if pref in (File.PREFERENCE.MERGE, File.PREFERENCE.OBJECT):
                self._bundle.logger.debug('   otr {}'.format(file_const))
                f.objects_to_record()

    def set_defaults(self):
        """Add default content to any file record that is empty"""

        for const_name, c in file_classes.items():
            if c.multiplicity == '1':
                f = self.file(const_name)
                if not f.record.unpacked_contents:
                    f.setcontent(f.default)

    def sync(self, force=None, defaults=False):
        raise NotImplementedError()

    def sync_out(self, file_const, fh=None):

        rtrn = False

        if not fh:
            from cStringIO import StringIO
            fh = StringIO()
            rtrn = True

        self.file(file_const).record_to_fh(file_const, fh)

        if rtrn:
            c = fh.getvalue()
            c.close()
            return c
        else:
            return None
