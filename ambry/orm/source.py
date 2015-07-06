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

class DelayedOpen(object):

    def __init__(self, source, fs, path, mode = 'r'):
        self._source = source
        self._fs = fs
        self._path = path
        self._mode = mode

    def open(self, mode = None ):
        return self._fs.open(self._path, mode if mode else self._mode )

    def syspath(self):
        return self._fs.getsyspath(self._path)

    def rowgen(self):
        return self._source.row_gen()

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
    widths = SAColumn('ds_widths', MutationList.as_mutable(JSONEncodedObj))
    description = SAColumn('ds_description', Text)
    file = SAColumn('ds_file', Text)
    urltype = SAColumn('ds_urltype', Text) # null or zip
    filetype = SAColumn('ds_filetype', Text) # tsv, csv, fixed
    encoding = SAColumn('ds_encoding', Text)
    url = SAColumn('ds_url', Text)
    ref = SAColumn('ds_ref', Text)
    hash = SAColumn('ds_hash', Text)
    source_generator = SAColumn('ds_sourcegen', Text) # Classname of of the source row generator

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
            parsed_path = parsed.path.replace('.zip','')
            root, ext = splitext(parsed_path)

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

    def fetch(self, cache_fs = None):
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

                fstor = DelayedOpen(self, fs, first, 'rU')
            else:
                import re

                # Put the walk output into a normal list of files
                for zip_fn in walk_all(fs):
                    if '_MACOSX' in zip_fn:
                        continue

                    if re.search(self.file, zip_fn):
                        fstor = DelayedOpen( self,  fs,zip_fn, 'rb')
                        break

        else:
            fstor = DelayedOpen(self, cache_fs, f, 'rb')

        self._fstor = fstor

        return fstor

    def row_gen(self, fstor = None):
        """Return a Row Generator"""
        import petl

        gft = self.get_filetype()

        if not fstor:
            fstor = self._fstor

        if gft == 'csv':
            return petl.io.csv.fromcsv(fstor, self.encoding if self.encoding else None)
        elif gft == 'tsv':
            return petl.io.csv.fromtsv(fstor, self.encoding if self.encoding else None)
        elif gft == 'fixed' or gft == 'txt':
            from ambry.util.fixedwidth import fixed_width_iter

            return fixed_width_iter(fstor.open(), self.widths)
        elif gft == 'xls':
            return petl.io.xls.fromxls(fstor.syspath(), sheet=self.segment if self.segment else None)
        elif gft == 'xlsx':
            return petl.io.xlsx.fromxlsx(fstor.syspath(), sheet=self.segment if self.segment else None)
        else:
            raise ValueError("Unknown filetype: {} ".format(gft))

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

