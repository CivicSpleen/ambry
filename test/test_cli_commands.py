__author__ = "Roman Suprotkin"
__email__ = "roman.suprotkin@developex.org"
import argparse

import ambry.cli
from test_base import TestBase
from test_cli import TestCLIMixin


class Test(TestCLIMixin, TestBase):
    parser = None
    logger_name = 'test_cli_command'

    def setUp(self):
        super(Test, self).setUp()
        # TODO: Create args to be passed in command
        self.parser = ambry.cli.get_parser()
        ambry.cli.global_logger = self.logger

    def format_args(self, *args):
        """
            @rtype: argparse.Namespace
        """
        return self.parser.parse_args(args, namespace=argparse.Namespace(config=self.config_file))

    def test_source_buildable(self):
        from ambry.cli import root
        from ambry.cli import source
        from ambry.cli import library

        args = self.format_args('info')
        print '== %s' % args
        root.root_command(args, self.rc)

        args = self.format_args('library', 'info')
        print '== %s' % args
        library.library_command(args, self.rc)

        args = self.format_args('library', 'drop')
        print '== %s' % args
        library.library_command(args, self.rc)

        args = self.format_args('library', 'sync', '-s')
        print '== %s' % args
        library.library_command(args, self.rc)

        args = self.format_args('list')
        print '== %s' % args
        root.root_command(args, self.rc)

        args = self.format_args('source', 'buildable', '-Fvid')
        print '== %s' % args
        source.source_command(args, self.rc)