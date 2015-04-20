"""Copyright (c) 2013 Clarinova.

This file is licensed under the terms of the Revised BSD License,
included in this distribution as LICENSE.txt

"""


from . import PartitionBase
from ..identity import PartitionIdentity, PartitionName
from ..database.csv import CsvDb


class CsvPartitionName(PartitionName):
    PATH_EXTENSION = '.csv'
    FORMAT = 'csv'


class CsvPartitionIdentity(PartitionIdentity):
    _name_class = CsvPartitionName


class CsvPartition(PartitionBase):

    """"""

    _id_class = CsvPartitionIdentity
    _db_class = CsvDb

    def __init__(self, bundle, record, **kwargs):
        super(CsvPartition, self).__init__(bundle, record)

    @property
    def database(self):

        if self._database is None:
            self._database = CsvDb(self.bundle, self, self.path)

        return self._database

    def create(self):
        self.database.create()

    def finalize(self):
        self.write_stats()

    def write_stats(self, min_key=None, max_key=None, count=None):
        """Assumes the partition is written without a header and that the first
        column is the id."""

        if not min_key and not max_key and not count:
            t = self.table

            if not t:
                return

            count = 0
            min_ = 2 ** 63
            max_ = -2 ** 63

            try:
                for row in self.database.reader():
                    count += 1
                    v = int(row[0])
                    min_ = min(min_, v)
                    max_ = max(max_, v)
            except ValueError:
                min_ = None
                max_ = None

            self.record.count = count
            self.record.min_key = min_
            self.record.max_key = max_
        else:
            if min_key:
                self.record.min_key = min_key

            if max_key:
                self.record.max_key = max_key

            if count:
                self.record.count = count

        with self.bundle.session as s:
            s.merge(self.record)

    def __repr__(self):
        return "<csv partition: {}>".format(self.name)
