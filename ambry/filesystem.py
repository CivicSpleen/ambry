"""Access objects for the file system within a bundle and for filesystem caches
used by the download processes and the library.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import os
import io

from ambry.orm import File
import zipfile

import urllib
import ambry.util

global_logger = ambry.util.get_logger(__name__)
# import logging; logger.setLevel(logging.DEBUG)


# makedirs
# Monkey Patch!
# Need to patch zipfile.testzip b/c it doesn't close file descriptors in 2.7.3
# The bug apparently exists in several other versions of python
# http://bugs.python.org/issue16408
def testzip(self):
    """Read all the files and check the CRC."""
    chunk_size = 2 ** 20
    for zinfo in self.filelist:
        try:
            # Read by chunks, to avoid an OverflowError or a
            # MemoryError with very large embedded files.
            f = self.open(zinfo.filename, "r")
            while f.read(chunk_size):     # Check CRC-32
                pass
            f.close()
            f._fileobj.close()  # This shoulnd't be necessary, but it is.
        except zipfile.BadZipfile:
            return zinfo.filename

zipfile.ZipFile.testzip = testzip


class DownloadFailedError(Exception):
    pass


class FileRef(File):

    """Extends the File orm class with awareness of the filsystem."""

    def __init__(self, bundle):

        self.super_ = super(FileRef, self)
        self.super_.__init__()

        self.bundle = bundle

    @property
    def abs_path(self):
        return self.bundle.filesystem.path(self.path)

    @property
    def changed(self):
        return os.path.getmtime(self.abs_path) > self.modified

    def update(self):
        self.modified = os.path.getmtime(self.abs_path)
        self.hash = Filesystem.file_hash(self.abs_path)
        self.bundle.database.session.commit()


class Filesystem(object):

    def __init__(self, config):
        self.config = config

    def get_cache_by_name(self, name):
        from dbexceptions import ConfigurationError
        from ckcache import new_cache

        config = self.config.filesystem(name)

        if not config:
            raise ConfigurationError(
                'No filesystem cache by name of {}'.format(name))

        return new_cache(config)

    @classmethod
    def find_f(cls, config, key, value):
        """Find a filesystem entry where the key `key` equals `value`"""

    @classmethod
    def rm_rf(cls, d):

        if not os.path.exists(d):
            return

        for path in (os.path.join(d, f) for f in os.listdir(d)):
            if os.path.isdir(path):
                cls.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)


class BundleFilesystem(Filesystem):

    BUILD_DIR = 'build'
    META_DIR = 'meta'

    def __init__(self, bundle, root_directory=None):

        super(BundleFilesystem, self).__init__(bundle.config)

        self.bundle = bundle
        if root_directory:
            self.root_directory = root_directory
        else:
            self.root_directory = Filesystem.find_root_dir()

    @staticmethod
    def find_root_dir(testFile='bundle.yaml', start_dir=None):
        """Find the parent directory that contains the bundle.yaml file."""
        import sys

        if start_dir is not None:
            d = start_dir
        else:
            d = sys.path[0]

        while os.path.isdir(d) and d != '/':

            test = os.path.normpath(d + '/' + testFile)

            if(os.path.isfile(test)):
                return d
            d = os.path.dirname(d)

        return None

    @property
    def root_dir(self):
        """Returns the root directory of the bundle."""
        return self.root_directory

    def ref(self, rel_path):

        s = self.bundle.database.session
        import sqlalchemy.orm.exc

        try:
            o = s.query(FileRef).filter(FileRef.path == rel_path).one()
            o.bundle = self.bundle

            return o
        except sqlalchemy.orm.exc.NoResultFound as e:
            raise e

    def path(self, *args):
        """Resolve a path that is relative to the bundle root into an absoulte
        path."""

        if len(args) == 0:
            raise ValueError("must supply at least one argument")

        args = (self.root_directory,) + args

        try:
            p = os.path.normpath(os.path.join(*args))
        except AttributeError as e:
            raise ValueError(
                "Path arguments aren't valid when generating path:" +
                e.message)
        dir_ = os.path.dirname(p)
        if not os.path.exists(dir_):
            try:
                # MUltiple process may try to make, so it could already exist
                os.makedirs(dir_)
            except Exception as e:  # @UnusedVariable
                pass

            if not os.path.exists(dir_):
                raise Exception("Couldn't create directory " + dir_)

        return p

    def build_path(self, *args):
        """Return a sub directory in the build area."""
        if len(args) > 0 and args[0] == self.BUILD_DIR:
            raise ValueError(
                "Adding build to existing build path " +
                os.path.join(
                    *
                    args))

        args = (self.bundle.build_dir,) + args
        return self.path(*args)

    @property
    def source_store(self):
        """Return the cache object for the store store, a location ( usually on
        the net ) where source files that can't be downloaded from the source
        agency can be stored."""

        return self.get_cache_by_name('source_store')

    @property
    def download_cache(self):
        return self.get_cache_by_name('downloads')

    def meta_path(self, *args):

        if len(args) > 0 and args[0] == self.META_DIR:
            raise ValueError(
                "Adding meta to existing meta path " +
                os.path.join(
                    *
                    args))

        args = (self.META_DIR,) + args
        return self.path(*args)

    def directory(self, rel_path):
        """Resolve a path that is relative to the bundle root into an absoulte
        path."""
        abs_path = self.path(rel_path)
        if(not os.path.isdir(abs_path)):
            os.makedirs(abs_path)
        return abs_path

    @staticmethod
    def file_hash(path):
        """Compute hash of a file in chunks."""
        import hashlib
        md5 = hashlib.md5()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5.update(chunk)
        return md5.hexdigest()

    def _get_unzip_file(self, cache, tmpdir, zf, path, name):
        """Look for a member of a zip file in the cache, and if it doesn't next
        exist, extract and cache it."""
        name = name.replace('..', '')

        if name.startswith('/'):
            name = name[1:]

        if name.endswith('/'):  # Its a ZIP file directory
            return None

        # If the file is comming from the download cache, be sure to use the entire
        # cache path, so files are always unique.
        download_cache = self.get_cache_by_name('downloads')

        if path.startswith(download_cache.cache_dir):
            base = path.replace(download_cache.cache_dir, '').lstrip('/')
        else:
            base = urllib.quote_plus(
                os.path.basename(path).replace(
                    '/',
                    '_'),
                '_')

        rel_path = os.path.join(
            base,
            urllib.quote_plus(
                name.replace(
                    '/',
                    '_'),
                '_'))

        # Check if it is already in the cache
        cached_file = cache.get(rel_path)

        if cached_file:
            return cached_file

        # Not in cache, extract it.
        tmp_abs_path = os.path.join(tmpdir, name)

        if not os.path.exists(tmp_abs_path):
            zf.extract(name, tmpdir)

        # Store it in the cache.

        abs_path = cache.put(tmp_abs_path, rel_path)

        # There have been zip files that have been truncated, but I don't know
        # why. this is a stab i the dark to catch it.
        if self.file_hash(tmp_abs_path) != self.file_hash(abs_path):
            raise Exception('Zip file extract error: md5({}) != md5({})'
                            .format(tmp_abs_path, abs_path))

        return abs_path

    def unzip(self, path, regex=None):
        """Context manager to extract a single file from a zip archive, and
        delete it when finished."""
        import tempfile
        import uuid

        if isinstance(regex, basestring):
            import re
            regex = re.compile(regex)

        cache = self.get_cache_by_name('extracts')

        tmpdir = os.path.join(cache.cache_dir, 'tmp', str(uuid.uuid4()))

        if not os.path.isdir(tmpdir):
            os.makedirs(tmpdir)

        try:
            with zipfile.ZipFile(path) as zf:
                abs_path = None
                if regex is None:
                    # Assume only one file in zip archive.
                    name = iter(zf.namelist()).next()
                    abs_path = self._get_unzip_file(
                        cache,
                        tmpdir,
                        zf,
                        path,
                        name)
                else:

                    for name in zf.namelist():
                        if regex.match(name):
                            abs_path = self._get_unzip_file(
                                cache,
                                tmpdir,
                                zf,
                                path,
                                name)
                            break

                return abs_path
        except zipfile.BadZipfile:
            self.bundle.error(
                "Error processing supposed zip file: '{}' You may want to delete it and try again. ".format(path))
            raise
        finally:
            self.rm_rf(tmpdir)

        return None

    def unzip_dir(self, path, regex=None):
        """Generator that yields the files from a zip file.

        Yield all the files in the zip, unless a regex is specified, in
        which case it yields only files with names that match the
        pattern.

        """
        import tempfile
        import uuid

        cache = self.get_cache_by_name('extracts')

        tmpdir = os.path.join(cache.cache_dir, 'tmp', str(uuid.uuid4()))

        if not os.path.isdir(tmpdir):
            os.makedirs(tmpdir)

        rtrn = True

        try:
            with zipfile.ZipFile(path) as zf:
                abs_path = None
                for name in zf.namelist():
                    # Noidea about this, but it seems useless.
                    if '__MACOSX' in name:
                        continue

                    abs_path = self._get_unzip_file(
                        cache,
                        tmpdir,
                        zf,
                        path,
                        name)

                    if not abs_path:
                        continue

                    if regex and regex.match(name) or not regex:
                        yield abs_path
        except Exception as e:
            self.bundle.error(
                "File '{}' can't be unzipped, removing it: {}".format(
                    path,
                    e))
            os.remove(path)
            raise
        finally:
            self.rm_rf(tmpdir)

    def download(self, url, test_f=None, unzip=False):
        """Context manager to download a file, return it for us, and delete it
        when done.

        url may also be a key for the sources metadata


        Will store the downloaded file into the cache defined
        by filesystem.download

        """

        import tempfile
        import urlparse
        import urllib2
        import urllib

        cache = self.get_cache_by_name('downloads')
        parsed = urlparse.urlparse(str(url))

        # If the URL doesn't parse as a URL, then it is a name of a source.
        if (not parsed.scheme and url in self.bundle.metadata.sources):

            source_entry = self.bundle.metadata.sources.get(url)

            # If a conversion exists, load it, otherwize, get the original URL
            if source_entry.conversion:
                url = source_entry.conversion
            else:
                url = source_entry.url
            parsed = urlparse.urlparse(str(url))

        if parsed.scheme == 'file':
            return parsed.path

        elif parsed.scheme == 's3':
            # To keep the rest of the code simple, we'll use the S# cache to generate a signed URL, then
            # download that through the normal process.
            from ckcache import new_cache

            s3cache = new_cache("s3://{}".format(parsed.netloc.strip('/')))

            url = s3cache.path(urllib.unquote_plus(parsed.path.strip('/')))
            parsed = urlparse.urlparse(str(url))
            use_hash = False
        else:
            use_hash = True

        # file_path = parsed.netloc+'/'+urllib.quote_plus(parsed.path.replace('/','_'),'_')
        file_path = os.path.join(parsed.netloc, parsed.path.strip('/'))

        # S3 has time in the query, so it never caches
        if use_hash and parsed.query:
            import hashlib

            hash = hashlib.sha224(parsed.query).hexdigest()
            file_path = os.path.join(file_path, hash)

        file_path = file_path.strip('/')

        # We download to a temp file, then move it into place when
        # done. This allows the code to detect and correct partial
        # downloads.
        download_path = os.path.join(
            tempfile.gettempdir(),
            file_path +
            ".download")

        def test_zip_file(f):
            if not os.path.exists(f):
                raise Exception("Test zip file does not exist: {} ".format(f))

            try:
                with zipfile.ZipFile(f) as zf:
                    return zf.testzip() is None
            except zipfile.BadZipfile:
                return False

        if test_f == 'zip':
            test_f = test_zip_file

        for attempts in range(3):

            if attempts > 0:
                self.bundle.error("Retrying download of {}".format(url))

            cached_file = None
            out_file = None
            excpt = None

            try:

                cached_file = cache.get(file_path)
                size = os.stat(cached_file).st_size if cached_file else None

                if cached_file and size:

                    out_file = cached_file

                    if test_f and not test_f(out_file):
                        cache.remove(file_path, True)
                        raise DownloadFailedError(
                            "Cached Download didn't pass test function " +
                            url)

                else:

                    self.bundle.log("Downloading " + url)
                    self.bundle.log(
                        "  --> " +
                        cache.path(
                            file_path,
                            missing_ok=True))

                    resp = urllib2.urlopen(url)
                    headers = resp.info()  # @UnusedVariable

                    if resp.getcode() is not None and resp.getcode() != 200:
                        raise DownloadFailedError(
                            "Failed to download {}: code: {} ".format(
                                url, resp.getcode()))

                    try:
                        out_file = cache.put(resp, file_path)
                    except:
                        self.bundle.error(
                            "Caught exception, deleting download file")
                        cache.remove(file_path, propagate=True)
                        raise

                    if test_f and not test_f(out_file):
                        cache.remove(file_path, propagate=True)
                        raise DownloadFailedError(
                            "Download didn't pass test function " +
                            url)

                break

            except KeyboardInterrupt:
                print "\nRemoving Files! \n Wait for deletion to complete! \n"
                cache.remove(file_path, propagate=True)
                raise
            except DownloadFailedError as e:
                self.bundle.error("Failed:  " + str(e))
                excpt = e
            except IOError as e:
                self.bundle.error(
                    "Failed to download " +
                    url +
                    " to " +
                    file_path +
                    " : " +
                    str(e))
                excpt = e
            except urllib.ContentTooShortError as e:
                self.bundle.error("Content too short for " + url)
                excpt = e
            except zipfile.BadZipfile as e:
                # Code that uses the yield value -- like th filesystem.unzip method
                # can throw exceptions that will propagate to here. Unexpected, but very useful.
                # We should probably create a FileNotValueError, but I'm lazy.
                self.bundle.error("Got an invalid zip file for " + url)
                cache.remove(file_path, propagate=True)
                excpt = e

            except Exception as e:
                self.bundle.error(
                    "Unexpected download error '" +
                    str(e) +
                    "' when downloading " +
                    str(url))
                cache.remove(file_path, propagate=True)
                raise

        if download_path and os.path.exists(download_path):
            os.remove(download_path)

        if excpt:
            raise excpt

        if unzip:

            if isinstance(unzip, bool):
                return self.unzip(out_file)
            elif unzip == 'dir':
                return self.unzip_dir(out_file)
            else:
                return self.unzip_dir(out_file, regex=unzip)

        else:
            return out_file

    def read_csv(self, f, key=None):
        """Read a CSV into a dictionary of dicts or list of dicts.

        Args:
            f a string or file object ( a FLO with read() )
            key columm or columns to use as the key. If None, return a list

        """
        import os.path

        opened = False
        if isinstance(f, basestring):
            if not f.endswith('.csv'):  # Maybe the name of a source
                if f in self.bundle.metadata.sources:
                    f = self.bundle.source(f)

            if not os.path.isabs(f):
                f = self.bundle.filesystem.path(f)

            f = open(f, 'rb')
            opened = True

        else:
            opened = False
            # f is already an open file

        import csv

        reader = csv.DictReader(f)

        if key is None:
            out = []
        else:
            if isinstance(key, (list, tuple)):
                def make_key(row):
                    return tuple(
                        [str(row[i].strip()) if row[i].strip() else None for i in key])
            elif callable(key):
                pass
            else:
                def make_key(row):
                    return row[key]

            out = {}

        for row in reader:

            if key is None:
                out.append(row)
            elif callable(key):
                k, v = key(row)
                out[k] = v
            else:
                out[make_key(row)] = row

        if opened:
            f.close

        return out

    def download_shapefile(self, url):
        """Downloads a shapefile, unzips it, and returns the .shp file path."""
        import os
        import re

        zip_file = self.download(url)

        if not zip_file or not os.path.exists(zip_file):
            raise Exception("Failed to download: {} ".format(url))

        file_ = None

        for file_ in self.unzip_dir(zip_file, regex=re.compile('.*\.shp$')):
            pass  # Should only be one

        if not file_ or not os.path.exists(file_):
            raise Exception(
                "Failed to unzip {} and get .shp file ".format(zip_file))

        return file_

    def load_yaml(self, *args):
        """Load a yaml file from the bundle file system. Arguments are passed to self.path()
        And if the first path element is not absolute, pre-pends the bundle path.

        Returns an AttrDict of the results.

        This will load yaml files the same way as RunConfig files.

        """
        from ambry.util import AttrDict

        f = self.path(*args)

        ad = AttrDict()
        ad.update_yaml(f)

        return ad

    def read_yaml(self, *args):
        """Straight-to-object reading of a YAML file.
        """
        import yaml

        from ambry.util import AttrDict

        with open(self.path(*args), 'rb') as f:
            return yaml.load(f)

    def write_yaml(self, o, *args):
        import yaml

        with open(self.path(*args), 'wb') as f:
            return yaml.safe_dump(
                o,
                f,
                default_flow_style=False,
                indent=4,
                encoding='utf-8')

    def get_url(self, source_url, create=False):
        """Return a database record for a file."""

        import sqlalchemy.orm.exc

        s = self.bundle.database.session

        try:
            o = (s.query(File).filter(File.source_url == source_url).one())

        except sqlalchemy.orm.exc.NoResultFound:
            if create:
                o = File(
                    source_url=source_url,
                    path=source_url,
                    process='none')
                s.add(o)
                s.commit()
            else:
                return None

        o.session = s  # Files have SavableMixin
        return o

    def get_or_new_url(self, source_url):
        return self.get_url(source_url, True)

    def add_file(self, rel_path):
        return self.filerec(rel_path, True)

    def filerec(self, rel_path, create=False):
        """Return a database record for a file."""

        import sqlalchemy.orm.exc

        s = self.bundle.database.session

        if not rel_path:
            raise ValueError('Must supply rel_path')

        try:
            o = (s.query(File).filter(File.path == rel_path).one())
            o._is_new = False
        except sqlalchemy.orm.exc.NoResultFound as e:

            if not create:
                raise e

            a_path = self.filesystem.path(rel_path)
            o = File(path=rel_path,
                     hash=Filesystem.file_hash(a_path),
                     modified=os.path.getmtime(a_path),
                     process='none'
                     )
            s.add(o)
            s.commit()
            o._is_new = True

        except Exception as e:
            return None

        return o


# Stolen from :
# https://bitbucket.org/fabian/filechunkio/src/79ba1388ee96/LICENCE?at=default

SEEK_SET = getattr(io, 'SEEK_SET', 0)
SEEK_CUR = getattr(io, 'SEEK_CUR', 1)
SEEK_END = getattr(io, 'SEEK_END', 2)

# A File like object that operated on a subset of another file. For use in Boto
# multipart uploads.


class FileChunkIO(io.FileIO):

    """A class that allows you reading only a chunk of a file."""

    def __init__(self, name, mode='r', closefd=True, offset=0, bytes_=None,
                 *args, **kwargs):
        """Open a file chunk.

        The mode can only be 'r' for reading. Offset is the amount of
        bytes_ that the chunks starts after the real file's first byte.
        Bytes defines the amount of bytes_ the chunk has, which you can
        set to None to include the last byte of the real file.

        """
        if not mode.startswith('r'):
            raise ValueError("Mode string must begin with 'r'")
        self.offset = offset
        self.bytes = bytes_
        if bytes_ is None:
            self.bytes = os.stat(name).st_size - self.offset
        super(FileChunkIO, self).__init__(name, mode, closefd, *args, **kwargs)
        self.seek(0)

    def seek(self, offset, whence=SEEK_SET):
        """Move to a new chunk position."""
        if whence == SEEK_SET:
            super(FileChunkIO, self).seek(self.offset + offset)
        elif whence == SEEK_CUR:
            self.seek(self.tell() + offset)
        elif whence == SEEK_END:
            self.seek(self.bytes + offset)

    def tell(self):
        """Current file position."""
        return super(FileChunkIO, self).tell() - self.offset

    def read(self, n=-1):
        """Read and return at most n bytes."""
        if n >= 0:
            max_n = self.bytes - self.tell()
            n = min([n, max_n])
            return super(FileChunkIO, self).read(n)
        else:
            return self.readall()

    def readall(self):
        """Read all data from the chunk."""
        return self.read(self.bytes - self.tell())

    def readinto(self, b):
        """Same as RawIOBase.readinto()."""
        data = self.read(len(b))
        n = len(data)
        try:
            b[:n] = data
        except TypeError as err:
            import array
            if not isinstance(b, array.array):
                raise err
            b[:n] = array.array(b'b', data)
        return n
