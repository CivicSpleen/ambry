"""
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

from os.path import join, isdir
from os import makedirs

class LibraryFilesystem(object):
    """Build directory names from the filesystem entries in the run configuration

    Each of the method will return a directory based on an entry in the configuration. The
    directory will be created with makedirs.
    """

    def __init__(self,  config):

        self._root = config.library.filesystem_root

        self._config = config

    def _compose(self, name, args):
        """Get a named filesystem entry, and extend it into a path with additional
        path arguments"""

        p = self._config.filesystem[name]

        if args:
            p = join(p, *args)\

        p = p.format(root=self._root)

        if not isdir(p):
            makedirs(p)

        return p

    @property
    def root(self):
        return self._root

    def downloads(self, *args):
        return self._compose('downloads',args)

    def extracts(self, *args):
        return self._compose('extracts',args)

    def python(self, *args):
        return self._compose('python',args)

    def source(self, *args):
        return self._compose('source',args)

    def build(self, *args):
        return self._compose('build',args)

    def logs(self, *args):
        return self._compose('logs',args)

    def search(self, *args):
        """For file-based search systems, like Whoosh"""
        return self._compose('search',args)

    def git(self, *args):
        """Git home directory, indicates that git is installed"""
        return self._compose('git',args)

    @property
    def database_dsn(self):
        """Substitute the root dir into the database DSN, for Sqlite"""

        if not self._config.library.database:
            return 'sqlite:///{root}/library.db'.format(root=self._root)

        return self._config.library.database.format(root=self._root)


