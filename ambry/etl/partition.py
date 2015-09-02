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

    return PartitionMsgpackDataFile(fs, path, stats=stats)



class PartitionMsgpackDataFile(object):
    """A reader and writer for Partition files in MessagePack format, which is about 60%  faster than unicode
     csv writing, and slightly faster than plain csv. """

    EXTENSION = '.msg'
    VERSION = 1
    MAGIC = 'AMBRMDF'
    HDF = struct.Struct('>7sHi')

    HEADER_TEMPLATE = {

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
        self._nrows = 0
        self._header = None

        self._writer = None

        self._reader = None
        self.header = None # Header on read, to conform to Pipe interface

    @property
    def path(self):
        return self._path

    @property
    def syspath(self):
        return self._fs.getsyspath(self.munged_path, allow_none=True)


    def close(self):
        if self._writer:
            self._writer.close()
            self._writer = None

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

    def reader(self):
        return PMDFReader(self._fs.open(self.munged_path, mode='rb'))

    def writer(self):
        return PMDFWriter(self._fs.open(self.munged_path, mode='wb'))


class PMDFWriter(object):

    def __init__(self, fh):

        from copy import deepcopy

        self._fh = fh
        self._zfh = None
        self._data_start = None

        self._n = 0

        self.file_header = deepcopy(PartitionMsgpackDataFile.HEADER_TEMPLATE)

    def set_row_header(self, headers):
        from copy import deepcopy

        schema = []

        for i, h in enumerate(headers):
            d = deepcopy(PartitionMsgpackDataFile.SCHEMA_TEMPLATE)
            d['pos'] = i
            d['name'] = h
            schema.append(d)

        self.file_header['schema'] = schema

    def write_file_header(self):

        """Write the magic number, version and the file_header dictionary.  """
        assert self._n == 0

        self._fh.seek(0)

        fhb = msgpack.packb(self.file_header, encoding='utf-8').encode('zlib')

        self._data_start = PartitionMsgpackDataFile.HDF.size + len(fhb)

        hdf = PartitionMsgpackDataFile.HDF.pack(
                    PartitionMsgpackDataFile.MAGIC,
                    PartitionMsgpackDataFile.VERSION,
                    self._data_start)

        assert len(hdf) == PartitionMsgpackDataFile.HDF.size

        self._fh.write(hdf)
        self._fh.write(fhb)

        assert self._fh.tell() == self._data_start

    def insert_row(self, row):

        if self._n == 0:
            self._fh.seek(self._data_start)
            self._zfh = gzip.GzipFile(fileobj=self._fh)

        self._n += 1

        # Assume the first item is the id, and fill it if it is empty
        if row[0] is None:
            row[0] = self._n

        self._zfh.write(msgpack.packb(row, default=PartitionMsgpackDataFile.encode_obj, encoding='utf-8'))

    def close(self):

        if self._fh:
            self._zfh.close()
            self._zfh = None
            self._fh = None



class PMDFReader(object):

    def __init__(self, fh):
        """Reads the file_header and prepares for iterating over rows"""

        self.magic, self.version, self.start_of_data = \
            PartitionMsgpackDataFile.HDF.unpack(fh.read(PartitionMsgpackDataFile.HDF.size))

        self.file_header = msgpack.unpackb(fh.read(self.start_of_data-fh.tell()).decode('zlib'), encoding='utf-8')

        assert fh.tell() == self.start_of_data

        self._fh = gzip.GzipFile(fileobj=fh)


    @property
    def row_header(self):
        return [ c['name'] for c in self.file_header['schema']]

    @property
    def dict_rows(self):
        """Generate rows from the file"""

        for i, row in enumerate(self.rows):
            if i == 0:
                continue

            yield dict(list(zip(self._header, row)))

    def __iter__(self):
        """Iterator for reading rows"""
        from ..etl import RowProxy

        try:

            unpacker = msgpack.Unpacker(self._fh, object_hook=PartitionMsgpackDataFile.decode_obj, encoding='utf-8')

            rp = RowProxy(self.row_header)

            for row in unpacker:
                yield rp.set_row(row)

        finally:
            if self._fh:
                self._fh.close()
                self._fh = None

    def close(self):
        if self._fh:
            self._fh.close()

