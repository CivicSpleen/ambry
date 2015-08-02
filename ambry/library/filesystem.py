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
            p = join(p, *args)

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

    def search(self, *args):
        """For file-based search systems, like Whoosh"""
        return self._compose('search',args)

    @property
    def remotes(self):

        return self._config.library().get('remotes', {})

    def remote(self, name):
        from fs.s3fs import S3FS
        from fs.opener import fsopendir
        from ambry.util import parse_url_to_dict

        r = self.remotes[name]

        # TODO: Hack the pyfilesystem fs.opener file to get credentials from a keychain
        # https://github.com/boto/boto/issues/2836
        if r.startswith('s3'):
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

            pd = parse_url_to_dict(r)

            account = self._config.account(pd['netloc'])

            s3 =  S3FS(
                bucket = pd['netloc'],
                prefix = pd['path'],
                aws_access_key = account['access'],
                aws_secret_key = account['secret'],

            )

            ssl.match_hostname = _old_match_hostname

            return s3

        else:
            return fsopendir(r, create_dir = True)