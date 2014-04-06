"""
Created on Aug 31, 2012

@author: eric
"""
import unittest
from bundles.testbundle.bundle import Bundle
from ambry.run import  RunConfig
from test_base import  TestBase  # @UnresolvedImport
from ambry.util import memoize

class Test(TestBase):

    test_dir = None

    def setUp(self):
        import os
        from ambry.run import  get_runconfig

        #self.test_dir = tempfile.mkdtemp(prefix='test_cli_')
        self.test_dir = '/tmp/test_cli'

        self.config_file =  os.path.join(self.test_dir, 'config.yaml')
        self.rc = get_runconfig((self.config_file,RunConfig.USER_ACCOUNTS))

    def tearDown(self):
        pass


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

        return output, logger


    def reset(self):
        from ambry.run import  get_runconfig
        import os, tempfile, shutil


        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

        os.makedirs(self.test_dir)

        self.config_file = self.new_config_file()

        self.rc = get_runconfig((self.config_file,RunConfig.USER_ACCOUNTS))

        self.library = self.get_library()

    def get_library(self, name = 'default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        config = self.rc.library(name)

        l =  new_library(config, reset = True)

        return l

    def new_config_file(self):
        import os
        import configs
        import bundles

        config_source = os.path.join(os.path.dirname(configs.__file__),'clitest.yaml')
        out_config = os.path.join(self.test_dir, 'config.yaml')

        with open(config_source,'r') as f_in:
            with open(out_config,'w') as f_out:
                s = f_in.read()
                f_out.write(s.format(
                    root=self.test_dir,
                    source=os.path.dirname(bundles.__file__)
                ))

        return out_config

    def cmd(self, *args):

        from ambry.cli import main

        args = [
            '-c',self.config_file
        ] + list(args)

        output, logger = self.setup_logging()

        main(args, logger)

        output.flush()
        out_val =  output.getvalue()
        output.close()
        return out_val


    def test_basic(self):

        self.reset()

        c = self.cmd

        c('library','info')
        c('library sync -s')
        c('list')

        st = self.library.source

        for k,ident in st.list().items():
            deps=ident.data['dependencies']
            dc = len(deps) if deps else 0
            print dc, ident
            if dc == 0:
                c('bundle -d {} build --clean '.format(ident.vid))
                c('bundle -d {} install '.format(ident.vid))

        # Build the one with a dependency
        id_ = 'd000001C'
        c('bundle -d {} build --clean '.format(id_))
        c('bundle -d {} install '.format(id_))

    def test_library(self):

        c = self.cmd

        out = c("list -Fvid")

        print out

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())