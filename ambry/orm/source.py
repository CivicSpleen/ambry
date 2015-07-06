"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'


from sqlalchemy import Column as SAColumn
from sqlalchemy import  Text, String, ForeignKey, INTEGER
from . import Base, MutationDict, MutationList, JSONEncodedObj

from . import Base,  DictableMixin

class DataSource(Base, DictableMixin):
    """A source of data, such as a remote file or bundle"""

    __tablename__ = 'datasources'

    d_vid = SAColumn('ds_d_vid', String(16), ForeignKey('datasets.d_vid'), nullable=False, primary_key=True)
    name = SAColumn('ds_name', Text, primary_key=True)

    t_vid = SAColumn('ds_t_vid', String(16), ForeignKey('tables.t_vid'), nullable=True)

    title = SAColumn('ds_title', Text)
    table_name = SAColumn('ds_table_name', Text)
    segment = SAColumn('ds_segment', Text)
    time = SAColumn('ds_time', Text)
    space = SAColumn('ds_space', Text)
    grain = SAColumn('ds_grain', Text)
    start_line = SAColumn('ds_start_line', INTEGER)
    end_line = SAColumn('ds_end_line', INTEGER)
    comment_lines = SAColumn('ds_comment_lines', MutationList.as_mutable(JSONEncodedObj))
    header_lines = SAColumn('ds_header_lines', MutationList.as_mutable(JSONEncodedObj))
    description = SAColumn('ds_description', Text)
    file = SAColumn('ds_file', Text)
    urltype = SAColumn('ds_urltype', Text)
    filetype = SAColumn('ds_filetype', Text)
    url = SAColumn('ds_url', Text)
    ref = SAColumn('ds_ref', Text)
    hash = SAColumn('ds_hash', Text)


    def get_filetype(self):
        from os.path import splitext
        import urlparse

        if self.filetype:
            return self.filetype

        if self.file:
            root, ext = splitext(self.file)

            return ext[1:]

        parsed = urlparse.urlparse(self.url)

        root, ext = splitext(parsed.path)

        if ext == '.zip':
            root, ext = splitext(parsed.path)

            return ext[1:]
        elif ext:
            return ext[1:]

        return None

    def get_urltype(self):
        from os.path import splitext
        import urlparse

        if self.urltype:
            return self.urltype

        if self.url:
            root, ext = splitext(self.url)
            return ext[1:]

        return None

    @property
    def dict(self):
        """A dict that holds key/values for all of the properties in the
        object.

        :return:

        """
        return {p.key: getattr(self, p.key) for p in self.__mapper__.attrs }

    def download(self, cache_fs = None):
        """Download the source and return a callable object that will open the file. """

        from fs.zipfs import ZipFS
        import os

        if cache_fs is None:
            cache_fs = self._cache_fs # Set externally by Bundle.

        fstor = None

        def walk_all(fs):
            return [os.path.join(e[0], x) for e in fs.walk() for x in e[1]]

        f = download(self.url, cache_fs)

        if self.get_urltype() == 'zip':

            fs = ZipFS(cache_fs.open(f, 'rb'))
            if not self.file:
                first = walk_all(fs)[0]
                fstor = lambda: fs.open(first)
            else:
                import re

                # Put the walk output into a normal list of files
                for zip_fn in walk_all(fs):
                    if '_MACOSX' in zip_fn:
                        continue

                    if re.search(self.file, zip_fn):
                        fstor = lambda: fs.open(zip_fn, 'rb')
                        break

        else:
            fstor = lambda: cache_fs.open(f, 'rb')

        return fstor

    def row_gen(self):
        """Return a Row Generator"""


def generate_xls_rows(file_name, segment, decode=None):
    from xlrd import open_workbook
    from xlrd.biffh import XLRDError

    def srow_to_list(self, row_num, s):
        """Convert a sheet row to a list"""

        values = []

        for col in range(s.ncols):
            if decode:
                v = s.cell(row_num, col).value
                if isinstance(v, basestring):
                    v = decode(v)
                values.append(v)
            else:
                values.append(s.cell(row_num, col).value)

        return values

    try:
        wb = open_workbook(file_name)
    except XLRDError:
        from zipfile import ZipFile
        # Usually b/c the .xls file is XML, but not zipped.

        file_name = file_name.replace('.xls', '.xml')

        wb = open_workbook(file_name)

    s = wb.sheets()[segment if segment else 0]

    for i in range(0, s.nrows):
        yield srow_to_list(i, s)



def generate_delimited_rows(self, file_name):

    if self.line_mangler:

        def lm(f):
            for l in f:
                yield self.line_mangler(self, l)

        f = lm(f)

    delimiter = ','
    dialect = None

    if self.encoding in (None, 'ascii', 'unknown'):
        import csv
        reader =  lambda f:  csv.reader(f, delimiter=delimiter, dialect=dialect)
    else:
        import unicodecsv
        reader =  lambda f: unicodecsv.reader(f, delimiter=delimiter, dialect=dialect, encoding=self.encoding)

    self.line_number = 0
    with open(file_name, 'rU') as f:
        for i, row in enumerate( reader(f)):
            self.line_number = i
            yield row

def download(url, cache_fs):
    import urlparse

    import os.path
    import requests
    from ambry.util import copy_file_or_flo

    parsed = urlparse.urlparse(str(url))

    cache_path = os.path.join(parsed.netloc, parsed.path.strip('/'))

    if parsed.query:
        import hashlib
        hash = hashlib.sha224(parsed.query).hexdigest()
        cache_path = os.path.join(cache_path, hash)

    if not cache_fs.exists(cache_path):

        r = requests.get(url, stream=True)

        cache_fs.makedir(os.path.dirname(cache_path),recursive=True, allow_recreate=True)

        with cache_fs.open(cache_path, 'wb') as f:
            copy_file_or_flo(r.raw, f)

    return cache_path