"""
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

from os.path import join, dirname, isdir
from os import makedirs


class LibraryFilesystem():
    """Build directory names from the filesystem entries in the run configuration

    Each of the method will return a directory based on an entry in the configuration. The
    directory wil lbe created with makedirs.
    """

    def __init__(self,  config):
        self._config = config # RunConfig

    def _compose(self, name, args):
        """Get a named filesystem entry, and extend it into a path with additional
        path arguments"""

        p = self._config.filesystem(name)

        if args:
            p =  join(p, *args)


        if not isdir(p):
            makedirs(p)

        return p


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
