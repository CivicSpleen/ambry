"""Object-Rlational Mapping classess, based on Sqlalchemy, for representing the
dataset, partitions, configuration, tables and columns.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
__docformat__ = 'restructuredtext en'

from collections import OrderedDict
import datetime
import hashlib
import os
from os.path import splitext
import re
import shutil
import ssl

from dateutil import parser

import requests

import petl

import gspread

from oauth2client.client import SignedJwtAssertionCredentials

from contextlib import closing

from six.moves.urllib.parse import urlparse
from six.moves.urllib.request import urlopen

from fs.zipfs import ZipFS
from fs.s3fs import S3FS

from sqlalchemy import Column as SAColumn
from sqlalchemy import Text, String, ForeignKey, INTEGER, UniqueConstraint
from sqlalchemy.orm import relationship

from xlrd import open_workbook, xldate_as_tuple

from ambry.etl import Pipe
from ambry.util import parse_url_to_dict
from ambry.util.flo import copy_file_or_flo

from .source_table import SourceTable
from .table import Table

from . import MutationList, JSONEncodedObj
from . import Base,  DictableMixin


class SourceError(Exception):
    pass


class DelayedOpen(object):

    def __init__(self, source, fs, path, mode='r', from_cache=False, account_accessor=None):
        self._source = source
        self._fs = fs
        self._path = path
        self._mode = mode
        self._account_accessor = account_accessor

        self.from_cache = from_cache

    def open(self, mode=None, encoding=None):
        return self._fs.open(self._path, mode if mode else self._mode, encoding=encoding)

    def syspath(self):
        return self._fs.getsyspath(self._path)

    def source_pipe(self):
        return self._source.row_gen()


class SourceRowGen(Pipe):
    """Holds a reference to a source record """

    def __init__(self, source, rowgen):
        self._source = source
        self._rowgen = rowgen

    def __iter__(self):
        for row in self._rowgen:
            yield row

    def __str__(self):
        return super(SourceRowGen, self).__str__() + ' source={} rowgen={}'.format(self._source.name, type(self._rowgen))


class DataSource(Base, DictableMixin):
    """A source of data, such as a remote file or bundle"""

    __tablename__ = 'datasources'

    id = SAColumn('ds_id', INTEGER, primary_key=True)

    d_vid = SAColumn('ds_d_vid', String(16), ForeignKey('datasets.d_vid'), nullable=False)
    name = SAColumn('ds_name', Text)
    title = SAColumn('ds_title', Text)

    st_id = SAColumn('ds_st_id', INTEGER, ForeignKey('sourcetables.st_id'), nullable=True)
    source_table_name = SAColumn('ds_st_name', Text)
    _source_table = relationship(SourceTable, backref='sources')

    t_vid = SAColumn('ds_t_vid', String(16), ForeignKey('tables.t_vid'), nullable=True)
    dest_table_name = SAColumn('ds_dt_name', Text)
    _dest_table = relationship(Table, backref='sources')

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
    urltype = SAColumn('ds_urltype', Text)  # null or zip
    filetype = SAColumn('ds_filetype', Text)  # tsv, csv, fixed
    encoding = SAColumn('ds_encoding', Text)
    url = SAColumn('ds_url', Text)
    ref = SAColumn('ds_ref', Text)
    hash = SAColumn('ds_hash', Text)

    generator = SAColumn('ds_generator', Text)  # class name for a Pipe to generator rows

    account_acessor = None

    __table_args__ = (
        UniqueConstraint('ds_d_vid', 'ds_name', name='_uc_ds_d_vid'),
    )

    def get_filetype(self):
        if self.filetype:
            return self.filetype

        if self.file:
            root, ext = splitext(self.file)
            return ext[1:]

        parsed = urlparse(self.url)

        root, ext = splitext(parsed.path)

        if ext == '.zip':
            parsed_path = parsed.path.replace('.zip', '')
            root, ext = splitext(parsed_path)

            return ext[1:]

        elif ext:
            return ext[1:]

        return None

    def get_urltype(self):

        if self.urltype:
            return self.urltype

        if self.url and self.url.startswith('gs://'):
            return 'gs'  # Google spreadsheet

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
        SKIP_KEYS = ('_source_table', '_dest_table', 'd_vid', 't_vid', 'st_id', 'dataset', 'hash')
        return OrderedDict(
            (p.key, getattr(self, p.key)) for p in self.__mapper__.attrs if p.key not in SKIP_KEYS)

    @property
    def row(self):

        # Use an Ordered Dict to make it friendly to creating CSV files.
        SKIP_KEYS = ('id', '_source_table', '_dest_table', 'd_vid', 't_vid', 'st_id', 'dataset', 'hash')

        d = OrderedDict(
            [(p.key, getattr(self, p.key)) for p in self.__mapper__.attrs if p.key not in SKIP_KEYS])
        return d

    def update(self, **kwargs):

        for k, v in list(kwargs.items()):
            if hasattr(self, k):
                setattr(self, k, v)

    def source_pipe(self, cache_fs=None, account_accessor=None):

        if self.generator:  # Get the source from the generator, not from a file.
            gen = eval(self.generator)
            return gen(self)
        else:
            return self.fetch(cache_fs, account_accessor=account_accessor).source_pipe()

    def fetch(self, cache_fs=None, account_accessor=None):
        """Download the source and return a callable object that will open the file. """

        if self.get_urltype() == 'gs':
            return DelayedOpen(self, None, None, None, None)

        if cache_fs is None:
            cache_fs = self._cache_fs  # Set externally by Bundle in ambry.bundle.bundle.Bundle#source

        fstor = None

        def walk_all(fs):
            return [os.path.join(e[0], x) for e in fs.walk() for x in e[1]]

        if not self.url:
            raise SourceError("Can't download; not url specified")

        f = download(self.url, cache_fs, account_accessor=account_accessor)

        if self.get_urltype() == 'zip':

            fs = ZipFS(cache_fs.open(f, 'rb'))
            if not self.file:
                first = walk_all(fs)[0]

                fstor = DelayedOpen(self, fs, first, 'rU', None)
            else:

                # Put the walk output into a normal list of files
                for zip_fn in walk_all(fs):
                    if '_MACOSX' in zip_fn:
                        continue

                    if re.search(self.file, zip_fn):
                        fstor = DelayedOpen(self, fs, zip_fn, 'rb', account_accessor)
                        break

        else:
            fstor = DelayedOpen(self, cache_fs, f, 'rb')

        self._fstor = fstor

        return fstor

    def row_gen(self, fstor=None):
        """Return a Row Generator"""

        if self.get_urltype() == 'gs':
            return SourceRowGen(self, google_iter(self))

        gft = self.get_filetype()

        if not fstor:
            fstor = self._fstor

        if gft == 'csv':
            return SourceRowGen(self, petl.io.csv.fromcsv(fstor, self.encoding if self.encoding else None))
        elif gft == 'tsv':
            return SourceRowGen(self, petl.io.csv.fromtsv(fstor, self.encoding if self.encoding else None))
        elif gft == 'fixed' or gft == 'txt':
            from ambry.util.fixedwidth import fixed_width_iter

            return SourceRowGen(self, fixed_width_iter(fstor.open(mode='r', encoding=self.encoding), self))
        elif gft == 'xls':
            return SourceRowGen(self, excel_iter(fstor.syspath(), self.segment))
        elif gft == 'xlsx':
            return SourceRowGen(self, excel_iter(fstor.syspath(), self.segment))
        else:
            raise ValueError('Unknown filetype for source {}: {} '.format(self.name, gft))

    @property
    def source_table(self):

        if not self._source_table:
            name = self.source_table_name if self.source_table_name else self.name
            st = self.dataset.source_table(name)
            if not st:
                st = self.dataset.new_source_table(name)

            assert bool(st)

            self._source_table = st

        return self._source_table

    @property
    def dest_table(self):
        from .exc import NotFoundError

        if not self._dest_table:
            name = self.dest_table_name if self.dest_table_name else self.name

            try:
                self._dest_table = self.dataset.table(name)
            except NotFoundError:
                self._dest_table = self.dataset.new_table(name)

        return self._dest_table

    @property
    def column_map(self):
        """For each column, map from the source header ( column name ) to the destination header """
        return self.source_table.column_map

    @property
    def column_index_map(self):
        """For each column, map from the source header ( column name ) to the column position ( index )  """
        return self.source_table.column_index_map

    @property
    def widths(self):
        return self.source_table.widths

    @property
    def headers(self):
        return self.source_table.headers


def excel_iter(file_name, segment):

    def srow_to_list(row_num, s):
        """Convert a sheet row to a list"""

        values = []

        for col in range(s.ncols):
            values.append(s.cell(row_num, col).value)

        return values

    wb = open_workbook(file_name)

    s = wb.sheets()[int(segment) if segment else 0]

    for i in range(0, s.nrows):
        yield srow_to_list(i, s)


def get_s3(url, account_accessor):
    # TODO: Hack the pyfilesystem fs.opener file to get credentials from a keychain
    # The monkey patch fixes a bug: https://github.com/boto/boto/issues/2836

    _old_match_hostname = ssl.match_hostname

    def _new_match_hostname(cert, hostname):
        if hostname.endswith('.s3.amazonaws.com'):
            pos = hostname.find('.s3.amazonaws.com')
            hostname = hostname[:pos].replace('.', '') + hostname[pos:]
        return _old_match_hostname(cert, hostname)

    ssl.match_hostname = _new_match_hostname

    pd = parse_url_to_dict(url)

    account = account_accessor(pd['netloc'])

    s3 = S3FS(
        bucket=pd['netloc'],
        # prefix=pd['path'],
        aws_access_key=account['access'],
        aws_secret_key=account['secret'],
    )

    # ssl.match_hostname = _old_match_hostname

    return s3


def download(url, cache_fs, account_accessor=None):

    parsed = urlparse(str(url))

    cache_path = os.path.join(parsed.netloc, parsed.path.strip('/'))

    if parsed.query:
        hash_ = hashlib.sha224(parsed.query).hexdigest()
        cache_path = os.path.join(cache_path, hash_)

    if not cache_fs.exists(cache_path):

        cache_fs.makedir(os.path.dirname(cache_path), recursive=True, allow_recreate=True)

        if url.startswith('s3:'):
            s3 = get_s3(url, account_accessor)
            pd = parse_url_to_dict(url)

            with cache_fs.open(cache_path, 'wb') as fout:
                with s3.open(pd['path'], 'rb') as fin:
                    copy_file_or_flo(fin, fout)

        elif url.startswith('ftp:'):
            with closing(urlopen(url)) as fin:
                with cache_fs.open(cache_path, 'wb') as fout:
                    shutil.copyfileobj(fin, fout)

        else:
            r = requests.get(url, stream=True)
            with cache_fs.open(cache_path, 'wb') as f:
                copy_file_or_flo(r.raw, f)

    return cache_path


def make_excel_date_caster(file_name):
    """Make a date caster function that can convert dates from a particular workbook. This is required
    because dates in Excel workbooks are stupid. """

    wb = open_workbook(file_name)
    datemode = wb.datemode

    def excel_date(v):
        try:
            year, month, day, hour, minute, second = xldate_as_tuple(float(v), datemode)
            return datetime.date(year, month, day)
        except ValueError:
            # Could be actually a string, not a float. Because Excel dates are completely broken.

            try:
                return parser.parse(v).date()
            except ValueError:
                return None

    return excel_date


def google_iter(source):

    json_key = source._library.config.account('google_spreadsheets')

    scope = ['https://spreadsheets.google.com/feeds']

    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)

    spreadsheet_key = source.url.replace('gs://', '')

    gc = gspread.authorize(credentials)

    sh = gc.open_by_key(spreadsheet_key)

    wksht = sh.worksheet(source.segment)

    for row in wksht.get_all_values():
        yield row
