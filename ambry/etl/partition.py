"""
Writing data to a partition

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import unicodecsv as csv

from ambry.etl.pipeline import Sink, Pipe

def new_partition_data_file(fs, path, stats = None):

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
        ext = '.msg'

    return ext_map[ext](fs, path, stats=stats)

class PartitionDataFile(object):
    """An accessor for files that hold Partition Data"""

    def __init__(self, fs, path, stats = None):
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
        self.stats = stats

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

    @property
    def syspath(self):
        return self._fs.getsyspath(self.munged_path,allow_none=True)

    def open(self, *args, **kwargs):
        self._file =  self._fs.open(self.munged_path, *args, **kwargs)
        return self._file

    def close(self):
        if self._file:
            self._file.close();
            self._file = None

    def delete(self):
        from fs.errors import ResourceNotFoundError
        try:
            self._fs.remove(self.munged_path)
        except ResourceNotFoundError:
            pass

    @property
    def rows(self):
        """Generate rows from the file"""
        return NotImplementedError()

    def clean(self):
        """Remove all of the rows in the file"""
        return self.delete()

    @property
    def size(self):
        """Return the size of the file, in data rows"""
        return self._fs.getsize(self.munged_path)


    @property
    def munged_path(self):
        if self._path.endswith(self.EXTENSION):
            return self._path
        else:
            return self._path+self.EXTENSION

class PartitionCsvDataFileReader(object):

    def __init__(self, f):
        import unicodecsv as csv

        self._f = f
        self.n = 0
        self.header = None
        self.reader = csv.reader(self._f)

    def __iter__(self):
        return self

    def next(self):
        """Generate rows from the file"""

        row = self.reader.next()

        if not self.header:
            self.header = row

        self.n += 1

        return row

    def close(self):
        self._f.close()

class PartitionCsvDataFile(PartitionDataFile):
    """An accessor for files that hold Partition Data"""

    EXTENSION = '.csv'

    def __init__(self, fs, path, stats=None):

        super(PartitionCsvDataFile, self).__init__(fs, path, stats)

        self._file = None
        self._reader = None
        self._writer = None
        self._nrows = 0

    def set_reader(self, reader):
        self._reader = reader

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

        if stream:
            return PartitionCsvDataFileReader(stream)
        else:
            return PartitionCsvDataFileReader(self.openr())

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

        # WARNING! This *must* be outside of the ==0 block, because it may open the file,
        # which will set _nrows for a non-empty file
        w = self.writer()

        if self._nrows == 0:
            w.writerow(row)
            self._nrows += 1

        if self.stats:
            self.stats.process_header(row)

    def insert_body(self, row):
        """
        Add a row to the file.

        :param row:
        :return:
        """

        # Assume the first item is the id, and fill it is if it empty
        if row[0] is None:
            row[0] = self._nrows

        self.writer().writerow(row)

        if self.stats:
            self.stats.process_body(row)

        self._nrows += 1

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

class PartitionMsgpackDataFileReader(object):

    def __init__(self, f, compress=True):

        self._f = f
        self.n = 0
        self.header = None
        self._compress = compress

        if self._compress:
            import gzip

            self._f = gzip.GzipFile(fileobj=self._f)

    def __iter__(self):

        import msgpack

        unpacker = msgpack.Unpacker(self._f, object_hook=self.decode_obj)

        for row in unpacker:
            assert isinstance(row, (tuple, list)), row

            if not self.header:
                self.header = row

            self.n += 1

            yield row

    @staticmethod
    def decode_obj(obj):
        import datetime

        if b'__datetime__' in obj:
            obj = datetime.datetime.strptime(obj["as_str"], "%Y%m%dT%H:%M:%S.%f")
        elif b'__date__' in obj:
            obj = datetime.datetime.strptime(obj["as_str"], "%Y%m%d")
        else:
            raise Exception("Unknown type on decode: {} ".format(obj))

        return obj


    def close(self):
        self._f.close()

class PartitionMsgpackDataFile(PartitionDataFile):
    """A reader and writer for Partition files in MessagePack format, which is about 60%  faster than unicode
     csv writing, and slightly faster than plain csv. """

    EXTENSION = '.msg'

    def __init__(self, fs, path, stats=None, compress = True):

        super(PartitionMsgpackDataFile, self).__init__(fs, path, stats)

        self._file = None
        self._reader = None
        self._writer = None
        self._compress = compress

    def close(self):
        """
        Release resources

        :return:
        """

        self._nrows = 0
        if self._file:
            self._file.close()
            self._file = None

    def open(self,  mode, compress=True):

        if not self._file:
            self._file = self._fs.open(self.munged_path, mode=mode)

            # Allow overriding the compression so we can read the file as compressed data,
            # for copying to other files.
            if self._compress and compress:
                import gzip
                self._file = gzip.GzipFile(fileobj=self._file)

        return self._file

    def insert_header(self, row):
        """
        Write the header, but only if the file is empty
        :param row:
        :return:
        """

        import msgpack

        if not self._nrows == 0:
            return

        self._header = list(row)

        self.open( mode='wb')

        assert isinstance(row, (list, tuple))

        self._file.write(msgpack.packb(row))

        self._nrows += 1

        if self.stats:
            self.stats.process_header(row)

    @staticmethod
    def encode_obj(obj):
        import datetime
        if isinstance(obj, datetime.datetime):
            return {'__datetime__': True, 'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")}
        if isinstance(obj, datetime.date):
            return {'__date__': True, 'as_str': obj.strftime("%Y%m%d")}
        else:
            raise Exception("Unknown type on encode: {}, {}".format(type(obj), obj))

        return obj

    def insert_body(self, row):
        """
        Add a row to the file. The first row must be a tuple or list, containing the header, to set the order of
        the fields, while subsequent rows can be lists or dicts.

        :param row:
        :return:
        """

        import msgpack

        # Assume the first item is the id, and fill it is if it empty
        if row[0] is None:
            row[0] = self._nrows

        assert isinstance(row, (tuple, list)), row

        self._file.write(msgpack.packb(row, default=self.encode_obj))

        self._nrows += 1

    def reader(self, stream=None):

        if stream:
            return PartitionMsgpackDataFileReader(stream, self._compress)
        else:
            return  PartitionMsgpackDataFileReader(self._fs.open(self.munged_path, mode='rb'), self._compress)

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




