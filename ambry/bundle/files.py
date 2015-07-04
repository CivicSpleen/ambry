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

class FileTypeError(Exception):
    """Bad file type"""

class BuildSourceFile(object):

    file_to_record = 'ftr'
    record_to_file = 'rtf'

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
            return self.file_to_record

        elif self.record.size and not self.exists():
            # Record exists, but not the FS
            return self.record_to_file

        if self.record.modified > self.fs_modtime():
            # Record is newer
            return self.record_to_file

        elif self.fs_modtime() > self.record.modified:
            # Filesystem is newer
            return self.file_to_record

        return None

    def sync(self):
        """Synchronize between the file in the file system and the fiel record"""

        from time import time
        sd = self.sync_dir()

        if  sd == self.file_to_record:
            self.fs_to_record()

        elif sd == self.record_to_file:
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
        with self._fs.open(fn_path) as f:
            fr.update_contents(msgpack.packb([row for row in csv.reader(f)]))

        fr.mime_type = 'application/msgpack'
        fr.source_hash = self.fs_hash()

        fr.modified = self.fs_modtime()

    def record_to_fs(self):
        """Create a filesystem file from a File"""
        import unicodecsv as csv

        import msgpack

        fr = self._dataset.bsfile(self._file_const)

        fn_path = file_name(self._file_const)

        with self._fs.open(fn_path, 'wb') as f:
            w = csv.writer(f)
            for row in msgpack.unpackb(fr.contents):
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

        d = msgpack.unpackb(fr.contents)

        with self._fs.open(fn_path, 'wb') as f:
            yaml.dump(d, default_flow_style=False)


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

        with self._fs.open(file_name(self._file_const), 'wb') as f:
            f.write(fr.contents)

class MetadataFile(DictBuildSourceFile):

    def record_to_objects(self):
        """Create config records to match the file metadata"""
        from ..util import AttrDict

        fr = self._dataset.bsfile(self._file_const)

        contents = fr.unpacked_contents

        ad = AttrDict(contents)

        # Get time that filessystem was synchronized to the File record.
        # Maybe use this to avoid overwriting configs that changed by bundle program.
        # fs_sync_time = self._dataset.config.sync[self._file_const][self.file_to_record]

        for key, value in ad.flatten():
            if value:
                self._dataset.config.metadata[key[0]]['.'.join(str(x) for x in key[1:])] = value

        self._dataset._database.commit()

        return ad

    def object_to_record(self):
        pass

class PythonSourceFile(StringSourceFile):

    def import_bundle_class(self):
        """Add the filesystem to the Python sys path with an import hook, then import
        to file as Python"""

        context = {}

        exec self._dataset.bsfile(self._file_const).contents in context

        return context['Bundle']

file_info_map = {
    File.BSFILE.BUILD : ('bundle.py',PythonSourceFile),
    File.BSFILE.BUILDMETA: ('meta.py',PythonSourceFile),
    File.BSFILE.DOC: ('documentation.md',StringSourceFile),
    File.BSFILE.META: ('bundle.yaml',MetadataFile),
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

def file_default(const):
    """Return the default content for the file"""

    import ambry.bundle.default_files as df
    import os

    path = os.path.join(os.path.dirname(df.__file__),  file_name(const))

    with open(path) as f:
        return f.read()


class BuildSourceFileAccessor(object):

    def __init__(self, dataset, filesystem = None):
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

    def sync(self):

        syncs = []

        for file_const, (file_name, clz) in  file_info_map.items():
            f = self.file(file_const)
            syncs.append((file_const,f.sync()))

        return syncs

    def sync_dirs(self):
        return [ (file_const, self.file(file_const).sync_dir() )
                 for file_const, (file_name, clz) in  file_info_map.items() ]




