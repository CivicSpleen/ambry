"""
Writing data to a partition

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

def new_partition_data_file(fs, path):

    ext_map = {
        PartitionCsvDataFile.EXTENSION : PartitionCsvDataFile,
        PartitionMsgpackDataFile.EXTENSION: PartitionMsgpackDataFile
    }

    from os.path import split, splitext, join

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

    def insert(self, row):
        """
        Add a row to the file

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

    def openr(self):
        """Open for reading"""
        if self._file:
            self._file.close()

        self._file = self._fs.open(self.munged_path, mode='rb')

        return self._file

    def openw(self):
        """Open for writing"""

        if self._file:
            self._file.close()

        self._file = self._fs.open(self.munged_path, mode='wb', buffering=1 * 1024 * 1024)

        return self._file

    def writer(self, stream=None):
        import unicodecsv as csv

        if not self._writer:


            if not stream:
                stream = self.openw()

            self._writer = csv.writer(stream)

        return self._writer

    def reader(self, stream = None):
        import unicodecsv as csv

        if not self._reader:

            if not stream:
                stream = self.openw()


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

    def insert(self, row):
        """
        Add a row to the file. The first row must be a tuple or list, containing the header, to set the order of
        the fields, while subsequent rows can be lists or dicts.

        :param row:
        :return:
        """

        if self._nrows == 0:
            assert isinstance(row, (list, tuple))
            self._header = list(row)

        if isinstance(row, (list, tuple)):
            self.writer().writerow(row)
        else:
            # Assume the row has a map interface, and write out the row in the order of the
            # column names provided in the header
            self.writer().writerow([row.get(k,None) for k in self._header])

        self._nrows += 1

    @property
    def rows(self):
        """Generate rows from the file"""

        self.close()

        for i, row in enumerate(self.reader):
            if i == 0:
                self._header = row
            self._nrows  = 1
            yield row

    @property
    def dict_rows(self):
        """Generate rows from the file"""

        self.close()

        for i, row in enumerate(self.reader):
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

    def insert(self, row):
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


class Inserter(object):

    def __init__(self, partition, datafile):
        self._partition = partition
        self._table = partition.table
        self.datafile = datafile
        self._header = [ c.name for c in self._table.columns ]

        self.datafile.insert(self._header) # The header is always inserted first

        self.row_num = 1

        self._stats = self.make_stats()

    def make_stats(self):
        from stats import Stats

        stats = Stats()

        for i,c in enumerate(self._table.columns):
            stats.add(i,c)

        return stats

    def insert(self, row):
        from sqlalchemy.engine.result import RowProxy

        if not row.get('id', False): # conflicts with passing in lists
            row['id'] = self.row_num

        # Convert dicts to lists in the order of the header
        if isinstance(row, dict):
            row = [ row.get(k,None) for k in self._header ]
        elif isinstance(row, list):
            pass
        elif isinstance(row, RowProxy):
            row = [ row.get(k,None) for k in self._header ]
        else:
            raise Exception("Don't know what the row is")

        self.datafile.insert(self._stats.process(row))
        self.row_num += 1

    def close(self):

        self._partition.state = self._partition.STATES.BUILT
        self._partition.set_stats(self._stats.stats())
        self._partition.set_coverage(self._stats.stats())
        self._partition.table.update_from_stats(self._stats.stats())
        self._partition._bundle.dataset.commit()

    def __enter__(self):

        self._partition.state = self._partition.STATES.BUILDING
        self._partition._bundle.dataset.commit()
        return self

    def __exit__(self, type_, value, traceback):

        if type_ and type_ != GeneratorExit:
            self._partition.state = self._partition.STATES.ERROR
            self._partition._bundle.dataset.commit()
            return False

        self.close()

        return True
