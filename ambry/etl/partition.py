"""
Writing data to a partition

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import unicodecsv as csv

from ambry.etl.pipeline import Sink, Pipe


def new_partition_data_file(fs, path):

    ext_map = {
        PartitionCsvDataFile.EXTENSION: PartitionCsvDataFile,
        PartitionMsgpackDataFile.EXTENSION: PartitionMsgpackDataFile
    }

    from os.path import split, splitext

    dn, file_ext = split(path)
    fn, ext = splitext(file_ext)

    if fs and not fs.exists(dn):
        fs.makedir(dn, recursive=True)

    if not ext:
        ext = '.csv'

    return ext_map[ext](fs, path)


class PartitionDataFile(object):
    """An accessor for files that hold Partition Data"""

    def __init__(self, fs, path):
        """
        Create a new acessor
        :param fs: a filesystem object
        :param path: Path to the file, without an extension. Directories in path will be created as needed
        :return:
        """

        self._fs = fs

        self._path = path
        self._nrows = 0
        self._header = None

    def insert_body(self, row):
        """
        Add a row to the file

        :param row:
        :return:
        """
        return NotImplementedError()

    def insert_header(self, row):
        """
        Add a header to the file. Skip it if the file already has data

        :param row:
        :return:
        """
        return NotImplementedError()

    def close(self):
        """
        Release resources

        :return:
        """
        return NotImplementedError()

    @property
    def path(self):
        return self._path

    def open(self, *args, **kwargs):
        return self._fs.open(self.munged_path, *args, **kwargs)

    @property
    def rows(self):
        """Generate rows from the file"""
        return NotImplementedError()

    def clean(self):
        """Remove all of the rows in the file"""
        return NotImplementedError()

    @property
    def size(self):
        """Return the size of the file, in data rows"""
        return NotImplementedError()

    @property
    def munged_path(self):
        if self._path.endswith(self.EXTENSION):
            return self._path
        else:
            return self._path+self.EXTENSION

class PartitionCsvDataFile(PartitionDataFile):
    """An accessor for files that hold Partition Data"""

    EXTENSION = '.csv'

    def __init__(self, fs, path):

        super(PartitionCsvDataFile, self).__init__(fs, path)

        self._file = None
        self._reader = None
        self._writer = None
        self._nrows = None

    def openr(self):
        """Open for reading"""
        if self._file:
            self._file.close()

        self._file = self._fs.open(self.munged_path, mode='rb')

        return self._file

    def openw(self):
        """Open for writing"""
        from fs.errors import ResourceNotFoundError

        if self._file:
            self._file.close()

        try:
            self._nrows = len(self._fs.getcontents(self.munged_path).splitlines())
            mode = 'ab'
        except ResourceNotFoundError:
            self._nrows = 0
            mode = 'wb'

        self._file = self._fs.open(self.munged_path, mode=mode, buffering=1 * 1024 * 1024)

        return self._file

    def writer(self, stream=None):

        if not self._writer:

            if not stream:
                stream = self.openw()

            self._writer = csv.writer(stream)

        return self._writer

    def reader(self, stream=None):

        if not self._reader:

            if not stream:
                stream = self.openr()

            self._reader = csv.reader(stream)

        return self._reader

    def close(self):
        """
        Release resources

        :return:
        """

        self._nrows = 0
        if self._file:
            self._file.close()
        self._writer = None
        self._reader = None

    def insert_header(self, row):
        """
        Write the header, but only if the file is empty
        :param row:
        :return:
        """

        assert isinstance(row, (list, tuple))
        self._header = list(row)

        w = self.writer()

        if self._nrows == 0:
            w.writerow(row)


    def insert_body(self, row):
        """
        Add a row to the file. The first row must be a tuple or list, containing the header, to set the order of
        the fields, while subsequent rows can be lists or dicts.

        :param row:
        :return:
        """

        if not isinstance(row, (list, tuple)):
            # Assume the row has a map interface, and write out the row in the order of the
            # column names provided in the header

            row = [row.get(k, None) for k in self._header]

        if row[0] is None:
            row[0] = self._nrows

        self.writer().writerow(row)

        self._nrows += 1

    @property
    def rows(self):
        """Generate rows from the file"""

        self.close()

        for i, row in enumerate(self.reader()):
            if i == 0:
                self._header = row
            self._nrows  = 1
            yield row

    @property
    def dict_rows(self):
        """Generate rows from the file"""

        self.close()

        for i, row in enumerate(self.reader()):
            if i == 0:
                self._header = row
                continue

            self._nrows = 1

            yield dict(zip(self._header, row))

    def clean(self):
        """Remove all of the rows in the file"""

        self.close()
        self._fs.remove(self._path)

    @property
    def size(self):
        """Return the size of the file, in data rows"""
        return NotImplementedError()


class PartitionMsgpackDataFile(PartitionDataFile):
    """A reader and writer for Partition files in MessagePack format, which is about 60%  faster than unicode
     csv writing, and slightly faster than plain csv. """

    EXTENSION = '.msg'

    def __init__(self, fs, path):

        super(PartitionMsgpackDataFile, self).__init__(fs, path)

        self._file = None
        self._reader = None
        self._writer = None

    def close(self):
        """
        Release resources

        :return:
        """

        self._nrows = 0
        if self._file:
            self._file.close()

    def insert_body(self, row):
        """
        Add a row to the file. The first row must be a tuple or list, containing the header, to set the order of
        the fields, while subsequent rows can be lists or dicts.

        :param row:
        :return:
        """

        import msgpack

        if not self._file:
            self._file = self._fs.open(self.munged_path, mode='wb')

        if self._nrows == 0: # Save the header
            assert isinstance(row, (list, tuple))
            self._header = list(row)

        if isinstance(row, (list, tuple)):
            self._file.write(msgpack.packb(row))
        else:
            # Assume the row has a map interface, and write out the row in the order of the
            # column names provided in the header
            self._file.write(msgpack.packb([row.get(k,None) for k in self._header]))

        self._nrows += 1

    @property
    def rows(self):
        """Generate rows from the file"""

        import msgpack

        with self._fs.open(self.munged_path, mode='wb') as f:
            unpacker = msgpack.Unpacker(f)

            for i, row in enumerate(unpacker):
                if i == 0:
                    self._header = row
                self._nrows  = 1
                yield row

    @property
    def dict_rows(self):
        """Generate rows from the file"""

        for i,row in enumerate(self.rows):
            if i == 0:
                continue

            yield dict(zip(self._header, row))

    def clean(self):
        """Remove all of the rows in the file"""

        self.close()
        self._fs.remove(self._path)

    @property
    def size(self):
        """Return the size of the file, in data rows"""
        return NotImplementedError()



