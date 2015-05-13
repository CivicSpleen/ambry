"""
Created on Jun 22, 2012

@author: eric
"""

# Monkey patch loggin, because I really don't understand logging

import ambry.util
from ambry.util import install_test_logger

ambry.util.get_logger = install_test_logger('/tmp/ambry-test.log')

import unittest
from  bundles.testbundle.bundle import Bundle
from ambry.identity import *
import time, logging
import ambry.util


global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)
logging.captureWarnings(True)

class TestBase(unittest.TestCase):

    server_url = None

    def setUp(self):
        import ambry.util
        import uuid
        import tempfile

        ambry.util.install_test_logger('/tmp/test.log')

        self.uuid = str(uuid.uuid4())

        self.tmpdir = tempfile.mkdtemp(self.uuid)

        self.delete_tmpdir = True

    def tearDown(self):
        import shutil

        super(TestBase, self).tearDown()

        if os.path.exists(self.tmpdir):
            if self.delete_tmpdir:
                shutil.rmtree(self.tmpdir)
            else:
                print "NOT DELETING:  {}".format(self.tmpdir)


    def bundle_dirs(self):

        bundle = Bundle()

        marker = bundle.filesystem.build_path('test-marker')
        build_dir = bundle.filesystem.build_path() + '/' # Slash needed for rsync
        save_dir = bundle.filesystem.build_path() + "-save/"

        return bundle, marker, build_dir, save_dir

    def delete_bundle(self):
        from ambry.util import rm_rf

        bundle, marker, build_dir, save_dir = self.bundle_dirs()

        rm_rf(build_dir)
        rm_rf(save_dir)

    def copy_or_build_bundle(self):
        """Set up a clean bundle build, either by re-building the bundle, or
        by copying it from a saved bundle directory """
        
        # For most cases, re-set the bundle by copying from a saved version. If
        # the bundle doesn't exist and the saved version doesn't exist, 
        # build a new one. 

        bundle, marker, build_dir, save_dir = self.bundle_dirs()

        idnt = bundle.identity

        if str(idnt.name.version) != "0.0.1":
            # Rebuild the bundle if the test_library.py:test_versions
            # script didn't reset the bundle at the end
            from ambry.util import rm_rf
            rm_rf(build_dir)
            rm_rf(save_dir)

        idnt = Identity.from_dict(dict(bundle.metadata.identity))


        bundle.metadata.identity = idnt.ident_dict
        bundle.metadata.names = idnt.names_dict

        bundle.metadata.write_to_dir()

        if not os.path.exists(marker):
            global_logger.info( "Build dir marker ({}) is missing".format(marker))
            # There is a good reason to create a seperate instance, 
            # but don't remember what it is ... 

            bundle.clean()
            bundle = Bundle()   
            if not os.path.exists(save_dir):
                global_logger.info( "Save dir is missing; re-build bundle. ")

                bundle.pre_prepare()
                bundle.prepare()
                bundle.post_prepare()

                if str(bundle.identity.name.version) != '0.0.1':
                    raise Exception("Can only save bundle if version is 0.0.1. This one is version: {} ".format(bundle.identity.name.version))


                bundle.pre_build()
                bundle.build()
                bundle.post_build()

                bundle.close()

                with open(marker, 'w') as f:
                    f.write(str(time.time()))
                # Copy the newly built bundle to the save directory    
                os.system("rm -rf {1}; rsync -arv {0} {1} > /dev/null ".format(build_dir, save_dir))

        # Always copy, just to be safe. 
        #global_logger.info(  "Copying bundle from {}".format(save_dir))
        os.system("rm -rf {0}; rsync -arv {1} {0}  > /dev/null ".format(build_dir, save_dir))



    def new_rand_file(self, path, size = 1024):

        dir_ = os.path.dirname(path)

        if not os.path.isdir(dir_):
            os.makedirs(dir_)


        with open(path,'w+') as f:
            for i in range(size):
                f.write(str(i%10)*1024)
                #f.write('\n')

        return path
