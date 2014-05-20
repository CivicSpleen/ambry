"""
Created on Jun 30, 2012

@author: eric
"""

import unittest
import os.path
from  testbundle.bundle import Bundle
from ambry.run import  get_runconfig
import logging
import ambry.util
from ambry.run import RunConfig

from test_base import  TestBase

global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)

class Test(TestBase):
 
    def setUp(self):
        import testbundle.bundle
        self.bundle_dir = os.path.dirname(testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'client-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'),
                                 RunConfig.USER_CONFIG
                                 ))

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

       
    def test_basic(self):
        
        from ambry.cache import new_cache
        
        c = new_cache(self.rc.filesystem('google'))
        
        print c
       
        print c.list()
       
       
        
       

    def x_test_basic(self):
        from ambry.client.bigquery import BigQuery
        bg =  BigQuery()
        
        
        import StringIO
        import os
        import shutil
        import tempfile
        import time
        from gslib.third_party.oauth2_plugin import oauth2_plugin
        
        import boto
        
        # URI scheme for Google Cloud Storage.
        GOOGLE_STORAGE = 'gs'
        # URI scheme for accessing local files.
        LOCAL_FILE = 'file'
        project_id = 128975330021

        header_values = {"x-goog-api-version": "2",
                "x-goog-project-id": project_id}


        uri = boto.storage_uri('', GOOGLE_STORAGE)
        for bucket in uri.get_all_buckets():
            print bucket.name

        uri = boto.storage_uri(bucket.name, GOOGLE_STORAGE)
        for obj in uri.get_bucket():
            print '%s://%s/%s' % (uri.scheme, uri.bucket_name, obj.name)




import boto

# URI scheme for Google Cloud Storage.
GOOGLE_STORAGE = 'gs'
# URI scheme for accessing local files.
LOCAL_FILE = 'file'
        

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())