"""

For developing these tests, you can run in the same environment as these tests by setting the AMBRY_CONFIG
env variable:

    export AMBRY_CONFIG=
"""
import logging
import os
import shutil
import sys
import shlex
import subprocess
import unittest

from StringIO import StringIO

import bundles
import configs
from ambry.run import RunConfig
from test_base import TestBase

# from ambry.util import memoize


class TestLoggingMixin(object):
    logger = None
    logger_name = 'test_cli'
    logging_handler = None
    logging_handler_class = logging.StreamHandler
    logging_handler_args = None
    logging_handler_kwargs = None
    logging_dir = None
    output = None

    def setUp(self):
        super(TestLoggingMixin, self).setUp()
        self.output, self.logger = self.setup_logging()

    def get_logging_handler(self):
        return self.logging_handler_class(*self.get_logging_handler_args(), **self.get_logging_handler_kwargs())

    def get_logging_handler_args(self, *args):
        return (self.logging_handler_args or []) + list(args)

    def get_logging_handler_kwargs(self, **kwargs):
        handler_kwargs = self.logging_handler_kwargs or {}
        handler_kwargs.update(kwargs)
        return handler_kwargs

    def get_logging_dir(self):
        return self.logging_dir or '/var/log'

    def setup_logging(self):
        logger = logging.getLogger(self.logger_name)

        template = "%(name)s %(levelname)s %(message)s"

        formatter = logging.Formatter(template)

        output = StringIO()

        ch = self.get_logging_handler()
        ch.setFormatter(formatter)
        ch.setLevel(logging.DEBUG)
        logger._stream = ch.stream

        logger.addHandler(ch)

        self.logging_handler = ch

        return output, logger


class TestCLIMixin(object):
    config_file = None
    library = None
    rc = None
    test_dir = '/tmp/test_cli'

    def setUp(self):
        super(TestCLIMixin, self).setUp()
        self.reset()

    def tearDown(self):
        super(TestCLIMixin, self).tearDown()
        self.output.close()

    def reset(self):
        from ambry.run import get_runconfig

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
        return new_library(config, reset=True)

    def new_config_file(self):
        """Copy the config file into place."""
        config_source = os.path.join(os.path.dirname(configs.__file__), 'clitest.yaml')
        out_config = os.path.join(self.test_dir, 'config.yaml')

        with open(config_source, 'r') as f_in:
            with open(out_config, 'w') as f_out:
                s = f_in.read()
                f_out.write(s.format(
                    root=self.test_dir,
                    source=os.path.dirname(bundles.__file__)
                ))

        return out_config


class Test(TestCLIMixin, TestLoggingMixin, TestBase):
    logging_handler_class = logging.FileHandler

    def get_logging_handler_args(self, *args):
        return [os.path.join(self.get_logging_dir(), '%s.log' % self.logger_name)]

    def get_logging_dir(self):
        return self.test_dir

    def cmd(self, *args):

        args = shlex.split(' '.join(args))
        args = [sys.executable, '-mambry.cli'] + ['-c', self.config_file] + list(args)
        print(' '.join(args))
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
        self.reset()

        c = self.cmd

        c('info')
        c('library', 'info')
        c('library drop')
        c('library sync -s')

        # Check that we have the example bundles, but not the built library
        self.assertIn('dIjqPRbrGq001', c('list'))
        self.assertNotIn('LS    d00H003', c('list'))
        self.assertIn('example.com-simple-0.1.3', c('list'))
        self.assertIn('example.com-random-0.0.2', c('list'))

        buildable = [x.strip() for x in c('source buildable -Fvid').splitlines()]

        for vid in buildable:
            c('bundle -d {} build --clean --install '.format(vid))

        # Now it should show up in the list.
        self.assertIn('LS     dHSyDm4MNR002     example.com-random-0.0.2', c('list'))

        c('library push')

        # Can't rebuild an installed library.
        with self.assertRaises(subprocess.CalledProcessError):
            c('bundle -d dHSyDm4MNR002 prepare --clean ')

    # Broken
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