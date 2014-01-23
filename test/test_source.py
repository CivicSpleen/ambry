"""
Created on Jun 30, 2012

@author: eric
"""

import unittest
import os.path
from test_base import  TestBase
from  testbundle.bundle import Bundle
from sqlalchemy import * #@UnusedWildImport
from ambry.run import  get_runconfig
from ambry.run import  RunConfig

from ambry.source.repository import new_repository

import logging
import ambry.util


logger = ambry.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 

class Test(TestBase):
 
    def setUp(self):
        import testbundle.bundle, shutil, os
        
        self.bundle_dir = os.path.dirname(testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'source-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'),
                                 RunConfig.USER_CONFIG))

        self.copy_or_build_bundle()

        bundle = Bundle()    

        print "Deleting: {}".format(self.rc.group('filesystem').root_dir)
        ambry.util.rm_rf(self.rc.group('filesystem').root_dir)

        bdir = os.path.join(self.rc.group('sourcerepo')['dir'],'testbundle')


        pats = shutil.ignore_patterns('build', 'build-save','*.pyc', '.git','.gitignore','.ignore','__init__.py')

        print "Copying test dir tree to ", bdir
        shutil.copytree(bundle.bundle_dir, bdir, ignore=pats)

        # Import the bundle file from the directory
        from ambry.run import import_file
        import imp
        rp = os.path.realpath(os.path.join(bdir, 'bundle.py'))
        mod = import_file(rp)
     
        dir_ = os.path.dirname(rp)
        self.bundle = mod.Bundle(dir_)

        print self.bundle.bundle_dir

    def tearDown(self):
        pass

    def testBasic(self):
        import random

        repo = new_repository(self.rc.sourcerepo('clarinova.data'))
    
    
        repo.bundle_dir = self.bundle.bundle_dir

        repo.delete_remote()
        import time
        time.sleep(3)
        repo.init()
        repo.init_remote()
        
        repo.push(repo.service.user, repo.service.password)
        
        
    def testSync(self):
        

        for repo in self.rc.sourcerepo.list:
            print repo.service.list()
        


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())