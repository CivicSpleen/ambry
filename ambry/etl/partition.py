"""
Writing data to a partition

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
import datetime
import time
import gzip

import msgpack
import struct

import unicodecsv as csv


def new_partition_data_file(fs, path, stats=None):
    from os.path import split, splitext

    assert bool(fs)

    dn, file_ext = split(path)
    fn, ext = splitext(file_ext)

    if fs and not fs.exists(dn):
        fs.makedir(dn, recursive=True)

    if not ext:
        ext = '.msg'

    return PartitionMsgpackDataFile(fs, path)

class PMDFError(Exception):
    pass

class GzipFile(gzip.GzipFile):

    def __init__(self, filename=None, mode=None, compresslevel=9, fileobj=None, mtime=None, end_of_data=None):
        super(GzipFile, self).__init__(filename, mode, compresslevel, fileobj, mtime)
        self._end_of_data = end_of_data

    def _read(self, size=1024):
        """Alters the _read method to stop reading new gzip members when we've reached the end of the row data. """

        if self._new_member and self._end_of_data and self.fileobj.tell() >= self._end_of_data:
            raise EOFError, "Reached EOF"
        else:
            return super(GzipFile, self)._read(size)


class PartitionMsgpackDataFile(object):
    """A reader and writer for Partition files in MessagePack format, which is about 60%  faster than unicode
     csv writing, and slightly faster than plain csv. """

    EXTENSION = '.msg'
    VERSION = 1
    MAGIC = 'AMBRMPDF'

    # 8s: Magic Number, H: Version,  I: Number of rows, Q: End of row / Start of meta
    FILE_HEADER_FORMAT = struct.Struct('>8sHIQ')

    META_TEMPLATE = {

        'schema': {},
        'geo':{
            'srs': None,
            'bb': None
        },
        'excel':{
            'datemode': None,
            'worksheet': None
        },
        'source':{
            'url': None,
            'fetch_time': None,
            'file_type': None,
            'inner_file': None
        },
        'row_spec':{
            'header__lines': 0,
            'start_line': None,
            'end_lines': None
        },
        'comments':{
            'header': None,
            'footer': None
        },
        'stats': {
            'columns': None,
            'rows': None
        }
    }

    SCHEMA_TEMPLATE = {
        'pos': None,
        'name': None,
        'type': None,
        'description': None
    }

    def __init__(self, fs, path):

        assert bool(fs)

        self._fs = fs

        assert bool(self._fs)

        self._path = path

        self._writer = None
        self._reader = None

        self._compress = True


    @property
    def path(self):
        return self._path

    @property
    def syspath(self):
        return self._fs.getsyspath(self.munged_path, allow_none=True)

    def open (self, *args, **kwargs):
        return self._fs.open(self.munged_path, *args, **kwargs)

    def delete(self):
        from fs.errors import ResourceNotFoundError

        try:
            self._fs.remove(self.munged_path)
        except ResourceNotFoundError:
            pass

    @property
    def size(self):
        """Return the size of the file, in data rows"""
        return self._fs.getsize(self.munged_path)

    @property
    def munged_path(self):
        if self._path.endswith(self.EXTENSION):
            return self._path
        else:
            return self._path + self.EXTENSION

    @staticmethod
    def encode_obj(obj):
        if isinstance(obj, datetime.datetime):
            return {'__datetime__': True, 'as_str': obj.isoformat()}
        elif isinstance(obj, datetime.date):
            return {'__date__': True, 'as_str': obj.isoformat()}
        elif isinstance(obj, datetime.time):
            return {'__time__': True, 'as_str': obj.strftime("%H:%M:%S")}
        elif hasattr(obj, 'render'):
            return obj.render()
        elif hasattr(obj, '__str__'):
            return str(obj)
        else:
            raise Exception("Unknown type on encode: {}, {}".format(type(obj), obj))


    @staticmethod
    def decode_obj(obj):

        if b'__datetime__' in obj:
            try:
                obj = datetime.datetime.strptime(obj["as_str"], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                # The preferred format is without the microseconds, but there are some lingering
                # bundle that still have it.
                obj = datetime.datetime.strptime(obj["as_str"], "%Y-%m-%dT%H:%M:%S.%f")
        elif b'__time__' in obj:
            obj = datetime.time(*list(time.strptime(obj["as_str"], "%H:%M:%S"))[3:6])
        elif b'__date__' in obj:
            obj = datetime.datetime.strptime(obj["as_str"], "%Y-%m-%d").date()
        else:
            raise Exception("Unknown type on decode: {} ".format(obj))

        return obj

    @property
    def reader(self):
        if not self._reader:
            self._reader = PMDFReader(self, self._fs.open(self.munged_path, mode='rb'), compress = self._compress)

        return self._reader

    @property
    def writer(self):
        if not self._writer:
            if self._fs.exists(self.munged_path):
                mode = 'r+b'
            else:
                mode = 'wb'

            self._writer = PMDFWriter(self, self._fs.open(self.munged_path, mode=mode), compress = self._compress)

        return self._writer



class PMDFWriter(object):

    MAGIC = PartitionMsgpackDataFile.MAGIC
    VERSION = PartitionMsgpackDataFile.VERSION
    FILE_HEADER_FORMAT = PartitionMsgpackDataFile.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = PartitionMsgpackDataFile.FILE_HEADER_FORMAT.size
    META_TEMPLATE = PartitionMsgpackDataFile.META_TEMPLATE
    SCHEMA_TEMPLATE = PartitionMsgpackDataFile.SCHEMA_TEMPLATE

    def __init__(self, parent, fh, compress = True):

        from copy import deepcopy

        assert fh

        self._parent = parent
        self._fh = fh
        self._compress = compress

        self._zfh = None # Compressor for writing rows
        self._data_start = self.FILE_HEADER_FORMAT_SIZE

        self._row_writer = None

        try:
            self.magic, self.version, self._i, self._data_end = \
                self.FILE_HEADER_FORMAT.unpack(self._fh.read(self.FILE_HEADER_FORMAT_SIZE))

            self._fh.seek(self._data_end)

            data = self._fh.read()

            self.meta = msgpack.unpackb(data.decode('zlib'), encoding='utf-8')

            self._fh.seek(self._data_end)

        except IOError:
            self._fh.seek(0)

            self._data_end = self._data_start

            self._i = 0

            self.meta = deepcopy(self.META_TEMPLATE)

    @property
    def headers(self):
        return [c['name'] for c in self.meta['schema']]

    @headers.setter
    def headers(self, headers):
        self.set_row_header(headers)

    @property
    def info(self):

        return dict(
            version=self.version,
            rows=self._i,
            start_of_data=self._data_start,
            end_of_data=self._data_end
        )

    def set_row_header(self, headers):
        from copy import deepcopy

        schema = []

        for i, h in enumerate(headers):
            d = deepcopy(self.SCHEMA_TEMPLATE)
            d['pos'] = i
            d['name'] = h
            schema.append(d)

        self.meta['schema'] = schema

    def write_file_header(self):
        """Write the magic number, version and the file_header dictionary.  """

        hdf = self.FILE_HEADER_FORMAT.pack(self.MAGIC,self.VERSION,self._i,self._data_end)

        assert len(hdf) == self.FILE_HEADER_FORMAT_SIZE

        self._fh.seek(0)

        self._fh.write(hdf)

        assert self._fh.tell() == self.FILE_HEADER_FORMAT_SIZE, (self._fh.tell(), self.FILE_HEADER_FORMAT_SIZE)

    def write_meta(self):

        self._fh.seek(self._data_end) # Should probably already be there.

        fhb = msgpack.packb(self.meta, encoding='utf-8').encode('zlib')
        self._fh.write(fhb)

    def insert_row(self, row):

        if self._i == 0:

            if not self.headers:
                raise PMDFError("Must set row headers before inserting rows")

            self.write_file_header()

            # Creating the GzipFile object will also write the Gzip header, about 21 bytes of data.

            if self._compress:
                self._zfh = GzipFile(fileobj=self._fh)  # Compressor for writing rows
            else:
                self._zfh = self._fh

            self._row_writer = lambda row: self._zfh.write(
                                msgpack.packb(row, default=PartitionMsgpackDataFile.encode_obj, encoding='utf-8'))

            # Row header is also the first row
            self._row_writer(self.headers)

        self._i += 1

        self._row_writer(row)


    def close(self):

        if self._fh:
            # First close the Gzip file, so it can flush, etc.

            if self._compress and self._zfh:
                self._zfh.close()

            self._data_end = self._fh.tell()
            self._zfh = None

            self.write_file_header()
            self._fh.seek(self._data_end)

            self.write_meta()

            self._fh.close()
            self._fh = None

            if self._parent:
                self._parent._writer = None

class PMDFReader(object):

    MAGIC = PartitionMsgpackDataFile.MAGIC
    VERSION = PartitionMsgpackDataFile.VERSION
    FILE_HEADER_FORMAT = PartitionMsgpackDataFile.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = PartitionMsgpackDataFile.FILE_HEADER_FORMAT.size
    META_TEMPLATE = PartitionMsgpackDataFile.META_TEMPLATE
    SCHEMA_TEMPLATE = PartitionMsgpackDataFile.SCHEMA_TEMPLATE

    def __init__(self, parent, fh, compress = True):
        """Reads the file_header and prepares for iterating over rows"""

        self._parent = parent
        self._fh = fh
        self._compress = compress

        self.magic, self.version, self.rows, self.end_of_data = \
            self.FILE_HEADER_FORMAT.unpack(self._fh.read(self.FILE_HEADER_FORMAT_SIZE))

        self.start_of_data = int(self._fh.tell())

        assert self.start_of_data == self.FILE_HEADER_FORMAT_SIZE

        if self._compress:
            self._zfh = GzipFile(fileobj=self._fh, end_of_data=self.end_of_data)
        else:
            self._zfh =self._fh

        self.unpacker = msgpack.Unpacker(self._zfh, object_hook=PartitionMsgpackDataFile.decode_obj, encoding='utf-8')

        self._meta = None

        self._i = 0

    @property
    def info(self):

        return dict(
            version = self.version,
            rows = self.rows,
            start_of_data = self.start_of_data,
            end_of_data = self.end_of_data
        )

    @property
    def meta(self):

        if self._meta is None:
            pos = self._fh.tell()

            self._fh.seek(self.end_of_data)

            # Using the _fh b/c I suspect that the GzipFile attached to self._zfh has state that would
            # get screwed up if you read from a new position

            data = self._fh.read()

            if data:

                self._meta = msgpack.unpackb(data.decode('zlib'), encoding='utf-8')

            else:
                self._meta = {}

            self._fh.seek(pos)

        return self._meta

    @property
    def dict_rows(self):
        """Generate rows from the file"""

        self._fh.seek(self.start_of_data)

        self.headers = self.unpacker.next()

        try:
            for row in self.unpacker:
                yield dict(list(zip(self.headers, row)))

        finally:
            self.close()

    def __iter__(self):
        """Iterator for reading rows"""
        from ..etl import RowProxy

        self._fh.seek(self.start_of_data)

        self.headers = self.unpacker.next()

        rp = RowProxy(self.headers)

        for i, row in enumerate(self.unpacker):
            yield rp.set_row(row)


    def close(self):
        if self._fh:
            self.meta # In case caller wants to read mea after close.
            self._fh.close()
            self._fh = None
            if self._parent:
                self._parent._reader = None

