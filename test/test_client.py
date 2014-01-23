"""
Created on Aug 31, 2012

@author: eric
"""
import unittest
import os.path
import logging 
import ambry.util
from  testbundle.bundle import Bundle
from ambry.run import  RunConfig
from test_base import  TestBase
from  ambry.client.rest import Rest #@UnresolvedImport
from ambry.library import QueryCommand, get_library

server_url = 'http://localhost:7979'

logger = ambry.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 

class Test(TestBase):
 
    def start_server(self, rc):
        '''Run the Bottle server as a thread'''
        from ambry.client.siesta import  API
        import ambry.server.main
        from threading import Thread
        import time
        from functools import  partial
        
        logger.info("Starting library server")
        # Give the server a new RunCOnfig, so we can use a different library. 
      
        server = Thread(target = partial(ambry.server.main.test_run, rc) )
   
        server.setDaemon(True)
        server.start()
        
        #ambry.server.bottle.debug()

        #
        # Wait until the server responds to requests
        #
        a = API(server_url)
        for i in range(1,10): #@UnusedVariable
            try:
                # An echo request to see if the server is running. 
                a.test.echo('foobar').get(bar='baz')
                break
            except:
                logger.info( 'Server not started yet, waiting')
                time.sleep(1)
                               
    def setUp(self):
        
        import shutil,os
        
        self.copy_or_build_bundle()
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')    
        
        self.bundle = Bundle()  
        self.bundle_dir = self.bundle.bundle_dir
        
        self.server_rc = RunConfig([os.path.join(self.bundle_dir,'server-test-config.yaml')])
        self.client_rc = RunConfig([os.path.join(self.bundle_dir,'client-test-config.yaml')])
        
        root = os.path.join(self.client_rc.filesystem.root_dir,'test')
        
        shutil.rmtree(root)
        
        #self.start_server(self.server_rc)

    def get_library(self):
        """Clear out the database before the test run"""
        
        l = get_library(self.client_rc, 'client')
    
        l.database.clean()
        
        l.logger.setLevel(logging.DEBUG) 
        
        return l
        

    def tearDown(self):
        '''Shutdown the server process by calling the close() API, then waiting for it
        to stop serving requests '''
        
        from ambry.client.siesta import  API
        import time
        
        # Wait for the server to shutdown
        a = API(server_url)
        for i in range(1,10): #@UnusedVariable
            try:
                a.test.close.get()
                #print 'Teardown: server still running, waiting'
                time.sleep(1)
            except:
                break

    def test_library_install(self):
        '''Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent'''
      
        l = self.get_library()
     
        l.put(self.bundle)
        l.put(self.bundle)
        
        r = l.get(self.bundle.identity)

        self.assertIsNotNone(r.bundle)
        self.assertTrue(r.bundle is not False)
        self.assertEquals(self.bundle.identity.id_, r.bundle.identity.id_)
        
        print "Stored: ",  r.bundle.identity.name
        
        l.remove(self.bundle)
        r = l.get(self.bundle.identity)
        self.assertFalse(r)
        
        #
        # Same story, but push to remote first, so that the removed
        # bundle will get loaded back rom the remote
        #
      
        l.put(self.bundle)
        l.push()
        r = l.get(self.bundle.identity)
        self.assertIsNotNone(r.bundle)
        l.remove(self.bundle)
        
        r = l.get(self.bundle.identity)
        self.assertIsNotNone(r.bundle)
        self.assertTrue(r.bundle is not False)
        self.assertEquals(self.bundle.identity.id_, r.bundle.identity.id_)
        

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())