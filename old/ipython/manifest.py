"""Manifest Magics for creating a manifest file asssociated with an IPython
notebook.

Each of the magics creates an entry in a manifest file in memory, which
is executed to build the warehouse.

"""

__author__ = 'eric'

from IPython.core.getipython import get_ipython
from IPython.core.magic import (
    Magics,
    magics_class,
    line_magic,
    cell_magic,
    line_cell_magic)
from collections import defaultdict
from ambry.util import memoize


class ManifestMagicsImpl(object):
    _manifest_ = None

    tag_line_numbers = dict()

    def __init__(self, manifest=None):
        ManifestMagicsImpl._manifest_ = manifest

    @property
    def _manifest(self):
        from ambry.warehouse.manifest import Manifest

        return ManifestMagicsImpl._manifest_

    @_manifest.setter
    def _manifest(self, value):
        ManifestMagicsImpl._manifest_ = value

    def parseargs(self, line):
        import argparse
        import shlex
        import re

        parser = argparse.ArgumentParser(
            prog='ipython manifest',
            description='',
            prefix_chars='-+')

        parser.add_argument(
            '-f',
            '--force',
            default=False,
            action='store_true',
            help='Force re-creation of tables and fiels that already exist')
        parser.add_argument(
            '-c',
            '--cwd',
            default=False,
            action='store_true',
            help='Use the current working directory for the warehouse base_dir')
        parser.add_argument(
            '-i',
            '--install',
            default=False,
            action='store_true',
            help='Install the manifest after loading')

        line = re.sub(r'#.*$', '', line)  # Remove comments

        return parser.parse_args(shlex.split(line))

    @property
    @memoize
    def logger(self):
        from ambry.util import get_logger
        import logging

        logger = get_logger(
            'ipython',
            clear=True,
            template="%(levelname)s %(message)s")
        logger.setLevel(logging.INFO)

        class ClearOutputAdapter(logging.LoggerAdapter):
            def process(self, msg, kwargs):
                from IPython.display import clear_output

                clear_output()
                return msg, kwargs

        logger = ClearOutputAdapter(logger, None)

        return logger

    def get_manifest(self, line, args):
        from ..run import get_runconfig
        from ..dbexceptions import ConfigurationError
        from ..warehouse.manifest import Manifest
        from ..library import new_library

        rc = get_runconfig()

        base_dir = None

        try:
            d = rc.filesystem('warehouse')
            base_dir = base_dir if base_dir else d['dir']

        except ConfigurationError:
            pass

        if args.cwd:
            import os

            base_dir = os.getcwd()

        if not base_dir:
            raise ConfigurationError(
                "Must specify -b for base directory, "
                "or set filesystem.warehouse in configuration")

        library_name = 'default'

        library = new_library(rc.library(library_name))

        force = args.force
        install_db = False

        return Manifest(
            '',
            self.logger,
            library=library,
            base_dir=base_dir,
            force=force,
            install_db=install_db)

    def tag_lineno(self, tag, args, cell=None):
        """Return a cached line number for single line tags."""

        start_line = dict(
            partitions=20,
            extract=40,
            view=60
        )

        tag = tag.strip().lower()
        args = args.strip().lower()

        if tag not in self.tag_line_numbers:
            self.tag_line_numbers[tag] = dict()
            self.tag_line_numbers[tag]['_max'] = 0

        tg = self.tag_line_numbers[tag]

        if args in tg:
            return tg[args] + start_line[tag]

        else:

            line = tg['_max']

            if cell:
                tg['_max'] = line + len(cell.splitlines()) + 1
            else:
                tg['_max'] = line + 1

            tg[args] = line

            return line + start_line[tag]

    def manifest(self, line, cell):
        """Cell magic for all of the opening lines, TITLE, UID, ACCESS, etc.

        Line number is always 0

        """

        # TBD: Parse the line to set manifest parameters

        args = self.parseargs(line)

        if not self._manifest_ or args.force:  # TBD re-get manifest if force
            self._manifest = self.get_manifest(line, args)

        self._manifest.sectionalize(cell.splitlines(), 0)

        if args.install:
            return self._manifest.install()

    def partitions(self, line, cell):
        """Partitions all start at line 20."""
        self._manifest.sectionalize(['PARTITIONS: {}'.format(
            line)] + cell.splitlines(), first_line=self.tag_lineno('partitions',
                                                                   line))

    def extract(self, line):
        """Single extract line."""
        self._manifest.sectionalize(
            ['EXTRACT: {}'.format(line)],
            first_line=self.tag_lineno('extract', line)
        )

    def view(self, line, cell):
        self._manifest.sectionalize(['VIEW: {}'.format(
            line)] + cell.splitlines(), first_line=self.tag_lineno('view', line,
                                                                   cell))

    def mview(self, line, cell):
        self._manifest.sectionalize(['MVIEW: {}'.format(
            line)] + cell.splitlines(), first_line=self.tag_lineno('view', line,
                                                                   cell))

    def show(self):
        return str(self._manifest)


@magics_class
class ManifestMagics(Magics):
    """Magics for creating Ambry manifests within an ipython notebook."""

    def __init__(self, shell):
        # You must call the parent constructor
        super(ManifestMagics, self).__init__(shell)
        self.impl = ManifestMagicsImpl()

    @cell_magic
    def manifest(self, line, cell):
        """Cell magic for all of the opening lines, TITLE, UID, ACCESS, etc.

        Line number is always 0

        """

        r = self.impl.manifest(line, cell)

        self.shell.user_ns['manifest'] = self.impl._manifest

        return r

    @cell_magic
    def partitions(self, line, cell):
        """Partitions all start at line 20."""
        self.impl.partitions(line, cell)

    @line_magic
    def extract(self, line):
        """Single extract line."""
        self.impl.extract(line)

    @cell_magic
    def view(self, line, cell):
        self.impl.view(line, cell)

    @cell_magic
    def mview(self, line, cell):
        self.impl.mview(line, cell)


ip = get_ipython()
if ip:
    ip.register_magics(ManifestMagics)
