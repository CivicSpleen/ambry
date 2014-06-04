'''
Created on Aug 31, 2012

@author: eric
'''

import unittest
import os.path
import logging 
import ambry.util
from  bundles.testbundle.bundle import Bundle
from ambry.run import  get_runconfig, RunConfig
from test_base import  TestBase
from ambry.library.query import QueryCommand
from ambry.library  import new_library
from ambry.util import rm_rf

global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)
logging.captureWarnings(True)

class Test(TestBase):
 
    def setUp(self):
        import bundles.testbundle.bundle

        rm_rf('/tmp/server')

        self.copy_or_build_bundle()

        self.bundle_dir = os.path.dirname(bundles.testbundle.bundle.__file__)

        self.rc = get_runconfig((os.path.join(self.bundle_dir, 'client-test-config.yaml'),
                                 os.path.join(self.bundle_dir, 'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS)
        )

        self.server_rc = get_runconfig((os.path.join(self.bundle_dir, 'server-test-config.yaml'),
                                        RunConfig.USER_ACCOUNTS))
         

        self.bundle = Bundle()  
        self.bundle_dir = self.bundle.bundle_dir

    def tearDown(self):
        from ambry.library import clear_libraries

        # new_library() caches the library, and the test runner uses different threads
        # which together with the sqlite driver's inability to use threads, can cause some problems.

        clear_libraries()

        self.stop_server()

    def get_library(self, name = 'default'):
        """Return the same library that the server uses. """
        from ambry.library import new_library

        config = self.server_rc.library(name)

        l =  new_library(config, reset = True)

        return l

    def web_exists(self,s3, rel_path):
    
        import requests
        import urlparse
        
        url  = s3.path(rel_path, method='HEAD')
      
        parts = list(urlparse.urlparse(url))
        qs = urlparse.parse_qs(parts[4])
        parts[4] = None
        
        url = urlparse.urlunparse(parts)
        r = requests.head(urlparse.urlunparse(parts), params=qs)

        self.assertEquals(200, r.status_code)
    
        return True

    def test_connection(self):
        '''
        Test some of the server's test functions
        :return:
        '''
        from ambry.client.rest import RemoteLibrary

        self.start_server()

        a = RemoteLibrary(self.server_url)

        self.assertEquals('foobar', a.get_test_echo('foobar'))
        self.assertEquals('foobar', a.put_test_echo('foobar'))

        with self.assertRaises(Exception):
            a.get_test_exception()

        r = a.get_root()

        self.assertIn('/tmp/server/remote', r['remotes'])

        #self.assertEquals('library-test', r['upstream']['prefix'])

    def test_install(self):

        from ambry.util import rm_rf

        root = self.rc.group('filesystem').root
        rm_rf(root)

        l = self.get_library()

        print l.info

        l.put_bundle(self.bundle)

        def cb(what, metadata, start):
            return
            print "PUSH ", what, metadata['name'], start

        for remote in l.remotes[0:3]:

            # This really should use update(), but it throws inscrutable exceptions.
            for f in l.files.query.state('pushed').all:
                f.state = 'new'
                l.files.merge(f)

            print 'Pushing to ', remote
            l.push(cb=cb, upstream=remote)

        l.purge()  # Remove the entries from the library

        l.sync_remotes(clean=True, remotes=l.remotes[0:3])

        r = l.resolve(self.bundle.identity.vid)

        self.assertEquals('diEGPXmDC8001', str(r.vid))

        r = l.resolve(self.bundle.partitions.all[0].identity.vid)

        self.assertEquals('diEGPXmDC8001', str(r.vid))
        self.assertEquals('piEGPXmDC8001001', str(r.partition.vid))

        ident, cache = l.locate(self.bundle.identity.vid)

        self.assertEquals('/tmp/server/remote', cache.repo_id)
        self.assertEquals(self.bundle.identity.vid, ident.vid)


    def test_resolve(self):
        from ambry.client.rest import RemoteLibrary

        l = self.get_library()
        l.purge()
        print l.info
        #
        # Check that the library can list datasets that are inserted externally
        #

        l.put_bundle(self.bundle)

        ident = self.bundle.identity

        # Local Library
        self.assertEquals(ident.vid, l.resolve(ident.vid).vid)
        self.assertEquals(ident.vid, l.resolve(ident.vname).vid)
        self.assertEquals(ident.vid, l.resolve(ident.cache_key).vid)
        self.assertEquals(ident.vid, l.resolve(ident.sname).vid)

        for p in self.bundle.partitions:
            print '--', p.identity.cache_key
            dsid = l.resolve(p.identity.vid)
            self.assertEquals(ident.vid, dsid.vid)
            self.assertEquals(p.identity.vid, dsid.partition.vid)

            dsid = l.resolve(p.identity.cache_key)

            if not dsid:
                ck = p.identity.cache_key
                l.resolve(ck)

            self.assertIsNotNone(dsid)
            self.assertEquals(ident.vid, dsid.vid)
            self.assertEquals(p.identity.vid, dsid.partition.vid)

        # Remote Library

        self.start_server()

        rl = RemoteLibrary(self.server_url)

        self.assertEquals(ident.vid, rl.resolve(ident.vid).vid)
        self.assertEquals(ident.vid, rl.resolve(ident.vname).vid)
        self.assertEquals(ident.vid, rl.resolve(ident.cache_key).vid)
        self.assertEquals(ident.vid, (rl.resolve(ident.sname).vid))

        for p in self.bundle.partitions:
            print '--',p.identity.cache_key
            dsid = rl.resolve(p.identity.vid)
            self.assertEquals(ident.vid, dsid.vid)
            self.assertEquals(p.identity.vid, dsid.partition.vid)

            dsid = rl.resolve(p.identity.cache_key)
            self.assertEquals(ident.vid, dsid.vid)
            self.assertEquals(p.identity.vid, dsid.partition.vid)

        print rl.resolve('source/dataset-subset-variation-0.0.1/geot1.geodb')

    def test_load(self):

        from ambry.run import  get_runconfig, RunConfig
        from ambry.client.rest import RemoteLibrary
        from ambry.cache import new_cache
        from ambry.util import md5_for_file
        from ambry.identity import Identity

        config = self.start_server()
        l = new_library(config)

        rl = RemoteLibrary(self.server_url)


        #
        # Check that the library can list datasets that are inserted externally
        #

        l.put_bundle(self.bundle)

        s = set([i.fqname for i in rl.list().values()])

        self.assertIn('source-dataset-subset-variation-0.0.1~diEGPXmDC8001', s)

        dsident = rl.dataset('diEGPXmDC8001')

        s = set([i.fqname for i in dsident.partitions.values()])

        self.assertEquals(4, len(s))

        self.assertIn('source-dataset-subset-variation-tthree-0.0.1~piEGPXmDC8003001', s)
        self.assertIn('source-dataset-subset-variation-geot1-geo-0.0.1~piEGPXmDC8001001', s)
        self.assertIn('source-dataset-subset-variation-geot2-geo-0.0.1~piEGPXmDC8002001', s)

        #
        # Upload the dataset to S3, clear the library, then load it back in
        #

        rc = get_runconfig((os.path.join(self.bundle_dir,'test-run-config.yaml'),RunConfig.USER_ACCOUNTS))
        cache = new_cache(rc.filesystem('cached-compressed-s3'))

        fn = self.bundle.database.path
        identity = self.bundle.identity
        relpath = identity.cache_key

        r = cache.put(fn, relpath, identity.to_meta(file=fn))


        self.assertTrue(bool(cache.has(relpath)))

        # clear the library.

        l.purge()
        self.assertNotIn('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',
                         set([i.fqname for i in rl.list()]))

        # Load from  S3, directly in to the local library

        identity.add_md5(md5_for_file(fn))

        l.load(identity.cache_key, identity.md5)

        self.assertIn('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',
                      set([i.fqname for i in rl.list().values()]))

        # Do it one more time, using the remote library

        l.purge()
        self.assertNotIn('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',
                         set([i.fqname for i in rl.list().values()]))

        rl.load_dataset(identity)

        self.assertIn('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',
                      set([i.fqname for i in rl.list().values()]))

        # Check that we can get the record from the library

        self.assertEquals(identity.vid, rl.resolve(identity.vid).vid)
        self.assertEquals(identity.vid, rl.resolve(identity.vname).vid)
        self.assertEquals(identity.vid, rl.resolve(identity.cache_key).vid)
        self.assertEquals(identity.vid, rl.resolve(identity.sname).vid)


    def test_push(self):
        from ambry.identity import Identity
        from functools import partial

        config = self.server_library_config()

        # Create the library so we can get the same remote config
        l = new_library(config)
        s3 = l.upstream.last_upstream()

        print l.info

        db = l.database
        db.enable_delete = True
        try:
            db.drop()
            db.create()
        except:
            pass

        s3 = l.upstream.last_upstream()
        s3.clean()

        l.put_bundle(self.bundle)

        def push_cb(expect, action, metadata, time):
            import json

            self.assertIn(action, expect)

            identity = Identity.from_dict(json.loads(metadata['identity']))
            print action, identity.cache_key

        def throw_cb(action, metadata, time):
            raise Exception("Push shonld not run")

        l.push(cb=partial(push_cb, ('Pushed', 'Pushing')))

        # ALl should be pushed, so should not run
        l.push(cb=throw_cb)

        # Resetting library, but not s3, should already have all
        # records
        db = l.database
        db.enable_delete = True
        db.drop()
        db.create()
        l.put_bundle(self.bundle)

        l.push(cb=partial(push_cb, ('Has')))

        self.web_exists(s3, self.bundle.identity.cache_key)

        for p in self.bundle.partitions:
            self.web_exists(s3, p.identity.cache_key)

        l.sync_upstream()



    def test_files(self):
        '''
        Test some of the server's file functions
        :return:
        '''

        from ambry.cache import new_cache
        from ambry.bundle import DbBundle

        fs = new_cache(self.server_rc.filesystem('rrc-fs'))
        fs.clean()
        remote = new_cache(self.server_rc.filesystem('rrc'))


        config = self.start_server()

        l = new_library(config)

        l.put_bundle(self.bundle)
        l.push()

        ident = self.bundle.identity
        ck = ident.cache_key

        # The remote is tied to the REST server, so it has the
        # bundle, but the new filesystem cache does not.

        self.assertFalse(fs.has(ck))
        self.assertTrue(remote.has(ck))

        # But if we tie them together, the FS cache should have it

        fs.upstream = remote
        self.assertTrue(fs.has(ck))

        path = fs.get(ck)

        b = DbBundle(path)
        self.assertEquals(ck, b.identity.cache_key)

        # It should have been copied, so the fs should still have
        # it after disconnecting.

        fs.upstream = None
        self.assertTrue(fs.has(ck))

    def test_remote_sync(self):
        from ambry.library import new_library

        vid = self.bundle.identity.vid

        config = self.server_library_config()

        # Create the library so we can get the same remote config
        server_l = new_library(config)

        server_l.put_bundle(self.bundle)

        # A library that connects to the server
        remote_l = new_library(self.server_rc.library("reader"))
        remote_l.purge()

        self.assertTrue(len(remote_l.list()) == 0)

        self.assertEquals(vid, server_l.resolve(vid).vid)
        self.assertIsNone(remote_l.resolve(vid))

        self.start_server()

        remote_l.sync_remotes()

        #print server_l.info
        #print remote_l.info

        r = remote_l.resolve(vid)

        self.assertEquals(vid, r.vid)

        b = remote_l.get(vid)

        for p in self.bundle.partitions:

            print "Check ", p.identity
            b = remote_l.get(p.identity.vid)
            self.assertTrue(p.identity.fqname, b.partition.identity.fqname)

        self.assertEqual(1, len(remote_l.list()))

        # Test out syncing.



    # =======================




    def _test_put_bundle(self, name, remote_config=None):
        from ambry.bundle import DbBundle
        from ambry.library.query import QueryCommand
        
        rm_rf('/tmp/server')
        
        self.start_server(remote_config)
        
        r = None #Rest(self.server_url, remote_config)
        
        bf = self.bundle.database.path

        # With an FLO
        response =  r.put(open(bf), self.bundle.identity)
        self.assertEquals(self.bundle.identity.id_, response.object.get('id'))
      
        # with a path
        response =  r.put( bf, self.bundle.identity)
        self.assertEquals(self.bundle.identity.id_, response.object.get('id'))

        for p in self.bundle.partitions.all:
            response =  r.put( open(p.database.path), p.identity)
            self.assertEquals(p.identity.id_, response.object.get('id'))

        # Now get the bundles
        bundle_file = r.get(self.bundle.identity,'/tmp/foo.db')
        bundle = DbBundle(bundle_file)

        self.assertIsNot(bundle, None)
        self.assertEquals('a1DxuZ',bundle.identity.id_)

        # Should show up in datasets list. 
        
        o = r.list()
   
        self.assertTrue('a1DxuZ' in o.keys() )
    
        o = r.find(QueryCommand().table(name='tone').partition(any=True))
      
        self.assertTrue( 'b1DxuZ001' in [i.id_ for i in o])
        self.assertTrue( 'a1DxuZ' in [i.as_dataset.id_ for i in o])





def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())