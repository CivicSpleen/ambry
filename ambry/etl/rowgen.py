"""The RowGenerator reads a file and yields rows, handling simple headers in CSV
files, and complex headers with receeding comments in Excel files.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
import logging

from ambry.etl import Pipe
from ambry.util import get_logger

from six.moves.urllib.parse import urlparse
from six.moves.urllib.request import urlopen

logger = get_logger(__name__, level=logging.INFO, propagate=False)


class SourceError(Exception):
    pass


def source_pipe(bundle, source):
    """Create a source pipe from a source ORM record"""

    if source.generator:  # Get the source from the generator, not from a file.

        import sys
        # Ambry.build comes from ambry.bundle.files.PythonSourceFile#import_bundle
        gen = eval(source.generator, globals(), sys.modules['ambry.build'].__dict__)
        return gen(bundle, source)
    else:

        if source.get_urltype() == 'gs':
            return GoogleSource(bundle, source)

        gft = source.get_filetype()

        if gft == 'csv':
            return CsvSource(bundle, source)
        elif gft == 'tsv':
            return TsvSource(bundle, source)
        elif gft == 'fixed' or gft == 'txt':
            return FixedSource(bundle, source)
        elif gft == 'xls':
            return ExcelSource(bundle, source)
        elif gft == 'xlsx':
            return ExcelSource(bundle, source)
        else:
            raise ValueError("Unknown filetype for source {}: {} ".format(source.name, gft))


class DelayedOpen(object):
    """A Lightweight wrapper to delay opening a PyFilesystem object until is it used. It is needed because
    The open() command on a filesystem directory, to produce the file object, also opens the file
    """
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


def fetch(source, cache_fs, account_accessor):
    """Download the source and return a callable object that will open the file. """

    from fs.zipfs import ZipFS
    import os

    if source.get_urltype() == 'gs':
        return DelayedOpen(source, None, None, None, None)

    fstor = None

    def walk_all(fs):
        return [os.path.join(e[0], x) for e in fs.walk() for x in e[1]]

    if not source.url:
        raise SourceError("Can't download; not url specified")

    logger.debug("Downloading: {}".format(source.url))
    f = download(source.url, cache_fs, account_accessor=account_accessor)

    logger.debug("Downloaded: {}".format(source.url))

    if source.get_urltype() == 'zip':
        logger.debug("Testing a zip file")
        fs = ZipFS(cache_fs.open(f, 'rb'))

        if not source.file:
            first = walk_all(fs)[0]

            fstor = DelayedOpen(source, fs, first, 'rU', None)
            logger.debug("FSTOR for zip file")
        else:
            import re

            # Put the walk output into a normal list of files
            for zip_fn in walk_all(fs):

                if '_MACOSX' in zip_fn:
                    continue

                if re.search(source.file, zip_fn):
                    fstor = DelayedOpen(source, fs, zip_fn, 'rb', account_accessor)
                    logger.debug("FSTOR for zip archive")
                    break

            if not fstor:
                from ambry.dbexceptions import ConfigurationError
                raise ConfigurationError('Failed to get file {} from archive {}'.format(source.file, f))

    else:
        fstor = DelayedOpen(source, cache_fs, f, 'rb')
        logger.debug("FSTOR for file")

    return fstor


class SourcePipe(Pipe):
    """A Source RowGen is the first pipe in a pipeline, generating rows from the original source. """

    def __init__(self, bundle, source):

        self._source = source
        self._cache_fs = bundle.library.download_cache
        self._account_accessor = bundle.library.config.account
        self._fstor = None

    def __iter__(self):
        rg = self._get_row_gen()
        self.start()
        for i, row in enumerate(rg):
            if i == 0:
                self.headers = row

            yield row

        self.finish()

    def fetch(self):
        if not self._fstor:
            self._fstor = fetch(self._source, self._cache_fs, self._account_accessor)

        return self._fstor

    def _get_row_gen(self):
        pass

    def start(self):
        pass

    def finish(self):
        pass

    def __str__(self):
        from ..util import qualified_class_name

        return qualified_class_name(self)


class CsvSource(SourcePipe):
    """Generate rows from a CSV source"""
    def _get_row_gen(self):
        import petl
        fstor = self.fetch()
        return petl.io.csv.fromcsv(fstor, self._source.encoding if self._source.encoding else None)


class TsvSource(SourcePipe):
    """Generate rows from a TSV ( Tab selerated value) source"""
    def _get_row_gen(self):
        import petl

        fstor = self.fetch()
        return petl.io.csv.fromtsv(fstor, self._source.encoding if self._source.encoding else None)


class FixedSource(SourcePipe):
    """Generate rows from a fixed-width source"""

    def fixed_width_iter(self, flo, source):

        parts = []
        self.headers = []  # THe header will be the column positions.
        for i, c in enumerate(source.source_table.columns):

            try:
                int(c.start)
                int(c.width)
            except TypeError:
                raise TypeError('Source table {} must have start and width values for {} column '
                                .format(source.source_table.name, c.source_header))

            parts.append('row[{}:{}]'.format(c.start - 1, c.start + c.width - 1))
            self.headers.append('{}:{}'.format(c.start - 1, c.start + c.width - 1))

        parser = eval('lambda row: [{}]'.format(','.join(parts)))

        yield source.source_table.headers

        for line in flo:
            yield [e.strip() for e in parser(line.strip())]

    def _get_row_gen(self):

        fstor = self.fetch()
        return self.fixed_width_iter(fstor.open(mode='r', encoding=self._source.encoding), self._source)

    def __iter__(self):
        rg = self._get_row_gen()
        self.start()
        for row in rg:
            yield row

        self.finish()


class ExcelSource(SourcePipe):
    """Generate rows from an excel file"""
    def _get_row_gen(self):
        fstor = self.fetch()
        return excel_iter(fstor.syspath(), self._source.segment)


class GoogleSource(SourcePipe):
    """Generate rows from a CSV source

    To read a GoogleSpreadsheet, you'll need to have an account entry for google_spreadsheets, and the
    spreadsheet must be shared with the client email defined in the credentials.

    Visit http://gspread.readthedocs.org/en/latest/oauth2.html to learn how to generate the cerdential file, then
    copy the entire contents of the file into the a 'google_spreadsheets' key in the accounts file.

    Them share the google spreadsheet document with the email addressed defined in the 'client_email' entry of
    the credentials.

    """
    def _get_row_gen(self):

        """"Iterate over the rows of a goodl spreadsheet. The URL field of the source must start with gs:// followed by
        the spreadsheet key. """
        import gspread
        from oauth2client.client import SignedJwtAssertionCredentials

        json_key = self.bundle.library.config.account('google_spreadsheets')

        scope = ['https://spreadsheets.google.com/feeds']

        credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)

        spreadsheet_key = self.source.url.replace('gs://', '')

        gc = gspread.authorize(credentials)

        sh = gc.open_by_key(spreadsheet_key)

        wksht = sh.worksheet(self.source.segment)

        for row in wksht.get_all_values():
            yield row


def excel_iter(file_name, segment):
    from xlrd import open_workbook

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
    from fs.s3fs import S3FS
    from ambry.util import parse_url_to_dict

    import ssl

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
        #prefix=pd['path'],
        aws_access_key=account['access'],
        aws_secret_key=account['secret'],
    )

    # ssl.match_hostname = _old_match_hostname

    return s3


def download(url, cache_fs, account_accessor=None):

    import os.path
    import requests
    from ambry.util.flo import copy_file_or_flo
    from ambry.util import parse_url_to_dict
    import filelock

    parsed = urlparse(str(url))

    cache_path = os.path.join(parsed.netloc, parsed.path.strip('/'))

    if parsed.query:
        import hashlib
        hash = hashlib.sha224(parsed.query).hexdigest()
        cache_path = os.path.join(cache_path, hash)

    if not cache_fs.exists(cache_path):

        cache_fs.makedir(os.path.dirname(cache_path), recursive=True, allow_recreate=True)

        lock_file = cache_fs.getsyspath(cache_path + '.lock')

        with filelock.FileLock(lock_file):

            if url.startswith('s3:'):
                s3 = get_s3(url, account_accessor)
                pd = parse_url_to_dict(url)

                with cache_fs.open(cache_path, 'wb') as fout:
                    with s3.open(pd['path'], 'rb') as fin:
                        copy_file_or_flo(fin, fout)

            elif url.startswith('ftp:'):
                import shutil
                from contextlib import closing

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

    from xlrd import open_workbook

    wb = open_workbook(file_name)
    datemode = wb.datemode

    def excel_date(v):
        from xlrd import xldate_as_tuple
        import datetime

        try:

            year, month, day, hour, minute, second = xldate_as_tuple(float(v), datemode)
            return datetime.date(year, month, day)
        except ValueError:
            # Could be actually a string, not a float. Because Excel dates are completely broken.
            from dateutil import parser

            try:
                return parser.parse(v).date()
            except ValueError:
                return None

    return excel_date
