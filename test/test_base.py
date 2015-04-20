"""
Created on Jun 22, 2012

@author: eric
"""
import unittest
from  bundles.testbundle.bundle import Bundle
from ambry.identity import * #@UnusedWildImport
import time, logging
import ambry.util


global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)
logging.captureWarnings(True)

class TestBase(unittest.TestCase):

    server_url = None

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
        global_logger.info(  "Copying bundle from {}".format(save_dir))
        os.system("rm -rf {0}; rsync -arv {1} {0}  > /dev/null ".format(build_dir, save_dir))


    def server_library_config(self, name='default'):

        config = self.server_rc.library(name)

        return config

    def start_server(self, config=None, name='default'):
        '''Run the Bottle server as a thread'''
        from ambry.client.siesta import  API
        import ambry.server.main
        from threading import Thread
        import time
        from functools import  partial
        from ambry.client.rest import RemoteLibrary

        if not config:
            config = self.server_library_config(name)

        self.server_url = "http://{}".format(config['host'])
        
        global_logger.info("Checking server at: {}".format(self.server_url))

        a = RemoteLibrary(self.server_url)

        #
        # Test to see of the server is already running. 
        #

        try:
            # An echo request to see if the server is running. 
            r = a.get_is_debug()
            
            if r.object:
                global_logger.info( 'Already running a debug server')
            else:
                global_logger.info( 'Already running a non-debug server')
    
            # We already have a server, so carry on
            return config
        except:
            # We'll get an exception refused eception if there is not server
            global_logger.info( 'No server, starting a local debug server')


        server = Thread(target = partial(ambry.server.main.test_run, config) )
        server.setDaemon(True)
        server.start()
        
        #ambry.server.bottle.debug()
        
        # Wait for the server to start
        for i in range(1,10): #@UnusedVariable
            try:
                # An echo request to see if the server is running. 
                r = a.get_test_echo('start_server')
                break
            except:
                global_logger.info( 'Server not started yet, waiting')
                time.sleep(1)
                               
        r = a.get_test_echo('start_server')
        
        return config
    
    def stop_server(self):
        '''Shutdown the server process by calling the close() API, then waiting for it
        to stop serving requests '''
        

        import socket
        import time
        import ambry.client.exceptions as exc
        from requests.exceptions import ConnectionError
        from ambry.client.rest import RemoteLibrary
        
        if not self.server_url:
            return
        
        a = RemoteLibrary(self.server_url)

        try:
            is_debug = a.get_is_debug()
        except ConnectionError:
            # Already closed:
            return
  
        if not is_debug:
            global_logger.info("Server is not debug, won't stop")
            return
        else:
            global_logger.info("Server at {} is debug, stopping".format(self.server_url))
       
        # Wait for the server to shutdown
        
        for i in range(1,10): #@UnusedVariable
            try:
                a.post_close()
                global_logger.info('Teardown: server still running, waiting')
                time.sleep(1)
            except socket.error:
                pass # Just means that the socket is already closed
            except IOError:
                pass # Probably just means that the socket is already closed
            except ConnectionError:
                pass # Another way the socket can be closed. Thrown by requests library.
            except Exception as e:
                global_logger.error("Got an exception while stopping: {} {}".format(type(e), e))
                break   
            
        time.sleep(2) # Let the socket clear
            

    def new_rand_file(self, path, size = 1024):

        dir_ = os.path.dirname(path)

        if not os.path.isdir(dir_):
            os.makedirs(dir_)


        with open(path,'w+') as f:
            for i in range(size):
                f.write(str(i%10)*1024)
                #f.write('\n')

        return path
