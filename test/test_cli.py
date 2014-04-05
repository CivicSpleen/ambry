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
        from ambry.util import temp_file_name
        import os, tempfile

        self.test_dir = tempfile.mkdtemp(prefix='test_cli_')

    def tearDown(self):
        pass

    @property
    @memoize
    def config_file(self):
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

        main(args)


    def test_basic(self):

        c = self.cmd

        c('library','info')
        c('library sync -s')


        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())