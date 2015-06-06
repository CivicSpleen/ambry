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

class FileTypeError(Exception):
    """Bad file type"""

class BuildSourceFile(object):

    def __init__(self, dataset, filesystem, file_const):
        """
        Construct a new Build Source File acessor
        :param dataset: The dataset that will hold File records
        :param filesystem: A FS filesystem abstraction object
        :param file_const: The BSFILE file contact
        :return:
        """
        self._dataset = dataset
        self._fs = filesystem
        self._file_const = file_const

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""
        raise NotImplementedError

    def record_to_fs(self):
        """Create a filesystem file from a File"""
        raise NotImplementedError

    def set_size_mod(self, file_rec, fn_path):
        from ambry.util import md5_for_file
        from fs.errors import NoSysPathError
        info = self._fs.getinfokeys(fn_path, "modified_time", "size")

        file_rec.modified = info['modified_time']
        file_rec.size = info['size']
        try:
            file_rec.hash = md5_for_file(self._fs.getsyspath(fn_path))
        except NoSysPathError:
            pass

class RowBuildSourceFile(BuildSourceFile):
    """A Source Build file that is a list of rows, like a spreadsheet"""

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""
        import unicodecsv as csv
        from StringIO import StringIO
        import msgpack

        sio = StringIO()

        fn_path = file_name(self._file_const)

        with self._fs.open(fn_path) as f:
            for row in csv.reader(f):
                sio.write(msgpack.packb(row))

        fr = self._dataset.bsfile(self._file_const)
        fr.contents = sio.getvalue()

        self.set_size_mod( fr, fn_path)

    def record_to_fs(self):
        """Create a filesystem file from a File"""
        import unicodecsv as csv
        from StringIO import StringIO
        import msgpack

        fr = self._dataset.bsfile(self._file_const)

        fn_path = file_name(self._file_const)

        with self._fs.open(fn_path, 'wb') as f:
            unpacker = msgpack.Unpacker(StringIO(fr.contents))
            w = csv.writer(f)
            for row in unpacker:
                w.writerow(row)


class DictBuildSourceFile(BuildSourceFile):
    """A Source Build file that is a list of rows, like a spreadsheet"""

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""

        from StringIO import StringIO
        import msgpack

        sio = StringIO()

        fn_path = file_name(self._file_const)
        fr = self._dataset.bsfile(self._file_const)

        if fn_path.endswith('.yaml'):
            import yaml

            with self._fs.open(fn_path) as f:
                sio.write(msgpack.packb(yaml.load(f)))
            fr.mime_type = 'application/yaml'
        else:
            raise FileTypeError("Unknown file type for : %s" % fn_path)

        fr.contents = sio.getvalue()
        self.set_size_mod(fr, fn_path)

    def record_to_fs(self):
        """Create a filesystem file from a File"""

        import msgpack
        import yaml

        fr = self._dataset.bsfile(self._file_const)

        fn_path = file_name(self._file_const)

        d = msgpack.unpackb(fr.contents)

        with self._fs.open(fn_path, 'wb') as f:
            yaml.dump(d, default_flow_style=False)


class StringSourceFile(BuildSourceFile):
    """A Source Build File that is a single file. """

    def fs_to_record(self):
        """Load a file in the filesystem into the file record"""

        fn_path = file_name(self._file_const)
        fr = self._dataset.bsfile(self._file_const)

        with self._fs.open(fn_path) as f:
            fr.contents = unicode(f.read())

        fr.mime_type = 'text/plain'

        self.set_size_mod(fr, fn_path)

    def record_to_fs(self):
        """Create a filesystem file from a File"""

        fr = self._dataset.bsfile(self._file_const)

        with self._fs.open(file_name(self._file_const), 'wb') as f:
            f.write(fr.contents)


file_info_map = {
    File.BSFILE.BUILD : ('bundle.py',StringSourceFile),
    File.BSFILE.BUILDMETA: ('meta.py',StringSourceFile),
    File.BSFILE.META: ('bundle.yaml',DictBuildSourceFile),
    File.BSFILE.SCHEMA: ('schema.csv',RowBuildSourceFile),
    File.BSFILE.COLMAP: ('column_map.csv',RowBuildSourceFile),
    File.BSFILE.SOURCES: ('sources.csv',RowBuildSourceFile)
}

def file_name(const):
    """Return the file name for a file constant"""
    return file_info_map[const][0]

def file_class(const):
    """Return the class for a file constant"""
    return file_info_map[const][1]

class BuildSourceFileAccessor(object):

    def __init__(self, dataset, filesystem = None):
        self._dataset = dataset
        self._fs = filesystem

    def file(self, const_name):

        fc = file_class(const_name)

        bsfile = fc(self._dataset, self._fs, const_name)

        return bsfile