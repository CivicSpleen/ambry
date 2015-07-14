"""Access objects for the file system within a bundle and for filesystem caches
used by the download processes and the library.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

import io
import zipfile
import urllib

import os
from ambry.orm import File
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
            while f.read(chunk_size):  # Check CRC-32
                pass
            f.close()
            f._fileobj.close()  # This shoulnd't be necessary, but it is.
        except zipfile.BadZipfile:
            return zinfo.filename


zipfile.ZipFile.testzip = testzip


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





class BundleFilesystem(object):
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

            if os.path.isfile(test):
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
            except Exception:
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
        if not os.path.isdir(abs_path):
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


    def read_csv(self, f, key=None):
        """Read a CSV into a dictionary of dicts or list of dicts.

        Args:
            f a string or file object ( a FLO with read() )
            key columm or columns to use as the key. If None, return a list

        """
        import os.path

        # opened = False
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

        except Exception:
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

    def __init__(self, name, mode='r', closefd=True, offset=0, bytes_=None, *args, **kwargs):
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
