"""

For developing these tests, you can run in the same environment as these tests
by setting the AMBRY_CONFIG env variable:

    export AMBRY_CONFIG=
"""
import unittest
from bundles.testbundle.bundle import Bundle
from ambry.run import RunConfig
from test_base import TestBase  # @UnresolvedImport
from ambry.util import memoize


class Test(TestBase):
    test_dir = None

    def setUp(self):
        import os
        from ambry.run import get_runconfig

        # self.test_dir = tempfile.mkdtemp(prefix='test_cli_')
        self.test_dir = '/tmp/test_cli'

        self.reset()

        self.output, self.logger = self.setup_logging()

    def tearDown(self):

        self.output.close()

    def setup_logging(self):
        from StringIO import StringIO
        import logging
        import ambry.cli
        import sys

        logger = logging.getLogger('test_cli')

        template = "%(name)s %(levelname)s %(message)s"

        formatter = logging.Formatter(template)

        output = StringIO()

        ch = logging.StreamHandler(stream=output)

        ch.setFormatter(formatter)
        ch.setLevel(logging.DEBUG)
        logger._stream = ch.stream

        logger.addHandler(ch)

        self.logging_handler = ch

        return output, logger

    def reset(self):
        from ambry.run import get_runconfig
        import os
        import tempfile
        import shutil

        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

        os.makedirs(self.test_dir)

        self.config_file = self.new_config_file()

        self.rc = get_runconfig((self.config_file, RunConfig.USER_ACCOUNTS))

        self.library = self.get_library()

    def get_library(self, name='default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        config = self.rc.library(name)

        l = new_library(config, reset=True)

        return l

    def new_config_file(self):
        """Copy the config file into place. """
        import os
        import configs
        import bundles

        config_source = os.path.join(os.path.dirname(configs.__file__),
                                     'clitest.yaml')
        out_config = os.path.join(self.test_dir, 'config.yaml')

        with open(config_source, 'r') as f_in:
            with open(out_config, 'w') as f_out:
                s = f_in.read()
                f_out.write(s.format(
                    root=self.test_dir,
                    source=os.path.dirname(bundles.__file__)
                ))

        return out_config

    def cmd(self, *args):

        from ambry.cli import main
        import subprocess
        import shlex

        args = shlex.split(' '.join(args))

        args = ['-c', self.config_file] + list(args)

        args = ['python', '-mambry.cli'] + args
        print "=== Execute: ", " ".join(args)
        try:
            s = subprocess.check_output(args)
        except subprocess.CalledProcessError as e:
            print "ERROR: ", e
            print e.output
            raise

        return s

    def assertInFile(self, s, fn):

        with open(fn) as f:
            return self.assertIn(s, '\n'.join(f.readlines()))

    def test_basic(self):

        c = self.cmd

        self.assertIn('sqlite:////tmp/test_cli/library.db', c('library', 'info'))
        self.assertIn('Database:  sqlite:////tmp/test_cli/library.db', c('library', 'info'))

    def test_sync_build(self):
        import os
        from subprocess import CalledProcessError

        self.reset()

        c = self.cmd

        c('info')

        c('library', 'info')

        c('library drop')

        c('library sync -s')

        # Check that we have the example bundles, but not the built library
        self.assertIn('S     dIjqPRbrGq001', c('list'))
        self.assertNotIn('LS    d00H003', c('list'))
        self.assertIn('example.com-simple-0.1.3', c('list'))
        self.assertIn('example.com-random-0.0.2', c('list'))

        buildable = [x.strip() for x in
                     c('source buildable -Fvid').splitlines()]

        for vid in buildable:
            c('bundle -d {} build --clean --install '.format(vid))

        # Now it should show up in the list.
        self.assertIn('LS     dHSyDm4MNR002     example.com-random-0.0.2', c('list'))

        c('library push')

        # Can't rebuild an installed library.
        with self.assertRaises(CalledProcessError):
            c('bundle -d dHSyDm4MNR002 prepare --clean ')

    def test_library(self):

        c = self.cmd

        out = c("list -Fvid")

        print out


def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(Test))
    return test_suite


if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())