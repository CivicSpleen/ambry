'''
Created on Aug 31, 2012

@author: eric
'''

import unittest
import os.path
import logging 
import ambry.util
from  testbundle.bundle import Bundle
from ambry.run import  RunConfig
from test_base import  TestBase
from ambry.library.query import QueryCommand
from ambry.library  import new_library
from ambry.util import rm_rf

logger = ambry.util.get_logger(__name__)
logger.setLevel(logging.DEBUG) 
logging.captureWarnings(True)

class Test(TestBase):
 
    def setUp(self):
        
        rm_rf('/tmp/server')

        self.copy_or_build_bundle()
        self.bundle_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)),'testbundle')    
        self.rc = RunConfig([os.path.join(self.bundle_dir,'client-test-config.yaml'),
                             os.path.join(self.bundle_dir,'bundle.yaml'),
                             RunConfig.USER_CONFIG])
         
        self.server_rc = RunConfig([os.path.join(self.bundle_dir,'server-test-config.yaml'),RunConfig.USER_CONFIG])
       
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

        self.assertEquals('devtest.sandiegodata.org', r['upstream']['bucket'])
        self.assertEquals('library-test', r['upstream']['prefix'])

    def test_resolve(self):
        from ambry.client.rest import RemoteLibrary

        l = self.get_library()
        l.purge()
        print l.info
        #
        # Check that the library can list datasets that are inserted externally
        #

        r = l.put(self.bundle)

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

        self.start_server()

        rl = RemoteLibrary(self.server_url)

        l = self.get_library()
        print l.info

        #
        # Check that the library can list datasets that are inserted externally
        #

        r = l.put(self.bundle)

        s = set([i.fqname for i in rl.list()])

        self.assertIn('source-dataset-subset-variation-0.0.1~diEGPXmDC8001', s)

        dsident = rl.dataset('diEGPXmDC8001')

        s = set([i.fqname for i in dsident.partitions.values()])

        self.assertEquals(11, len(s))

        self.assertIn('source-dataset-subset-variation-hdf5-hdf-0.0.1~piEGPXmDC800f001', s)
        self.assertIn('source-dataset-subset-variation-tthree-3-0.0.1~piEGPXmDC800d001', s)
        self.assertIn('source-dataset-subset-variation-geot1-geo-0.0.1~piEGPXmDC8002001', s)

        #
        # Upload the dataset to S3, clear the library, then load it back in
        #

        rc = get_runconfig((os.path.join(self.bundle_dir,'test-run-config.yaml'),RunConfig.USER_CONFIG))
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
                      set([i.fqname for i in rl.list()]))

        # Do it one more time, using the remote library

        l.purge()
        self.assertNotIn('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',
                         set([i.fqname for i in rl.list()]))

        rl.load_dataset(identity)

        self.assertIn('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',
                      set([i.fqname for i in rl.list()]))

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

        # ALl should be pushed, so suhould not run
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


    def test_caches(self):
        '''Basic test of put(), get() and has() for all cache types'''
        from functools import partial
        from ambry.run import  get_runconfig, RunConfig
        from ambry.filesystem import Filesystem
        from ambry.cache import new_cache
        from ambry.util import md5_for_file
        from ambry.bundle import DbBundle

        #self.start_server() # For the rest-cache

        #fn = '/tmp/1mbfile'
        #with open(fn, 'wb') as f:
        #    f.write('.'*(1024))

        fn = self.bundle.database.path

        # Opening the file might run the database updates in
        # database.sqlite._on_connect_update_schema, which can affect the md5.
        b = DbBundle(fn)

        md5 = md5_for_file(fn)

        print "MD5 {}  = {}".format(fn, md5)

        rc = get_runconfig((os.path.join(self.bundle_dir,'test-run-config.yaml'),RunConfig.USER_CONFIG))

        for i, fsname in enumerate(['fscache', 'limitedcache', 'compressioncache',
                                    'cached-s3', 'cached-compressed-s3']):

            config = rc.filesystem(fsname)
            cache = new_cache(config)
            print '---', fsname, cache
            identity = self.bundle.identity

            relpath = identity.cache_key

            r = cache.put(fn, relpath,identity.to_meta(md5=md5))

            r = cache.get(relpath)

            if not r.startswith('http'):
                self.assertTrue(os.path.exists(r), 'Not a url: {}: {}'.format(r,str(cache)))

            self.assertTrue(cache.has(relpath, md5=md5))

            cache.remove(relpath, propagate=True)

            self.assertFalse(os.path.exists(r), str(cache))
            self.assertFalse(cache.has(relpath))


        cache = new_cache(rc.filesystem('s3cache-noupstream'))
        r = cache.put(fn, 'a')


    def test_simple_install(self):
        from ambry.client.rest import RemoteLibrary
        from ambry.cache.remote import RestReadCache
        
        config = self.start_server()

        # Create the library so we can get the same remote config
        l = new_library(config)

        s3 = l.upstream.last_upstream()

        s3.clean()

        print "S3 cache ", str(s3)

        if not s3.has(self.bundle.identity.cache_key):
            print 'Uploading: ', self.bundle.identity.cache_key
            s3.put(self.bundle.database.path,self.bundle.identity.cache_key)
            self.web_exists(s3,self.bundle.identity.cache_key)

        for p in self.bundle.partitions:
            if not s3.has(p.identity.cache_key):
                print 'Uploading: ', p.identity.cache_key
                s3.put(p.database.path,p.identity.cache_key)
                self.web_exists(s3,p.identity.cache_key)
            else:
                print 'Has      : ', p.identity.cache_key

        #
        # Kick the remote library to load the dataset
        #
        rl = RemoteLibrary(self.server_url)
        ident = self.bundle.identity
        ident.add_md5(file=self.bundle.database.path)
        rl.load_dataset(ident)
        self.assertIn('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',
                      set([i.fqname for i in rl.list()]))




        return

        # Try variants of find. 
        r = api.find(self.bundle.identity.name)
        self.assertEquals(self.bundle.identity.name, r[0].name)
        
        r = api.find(QueryCommand().identity(name = self.bundle.identity.name))
        self.assertEquals(self.bundle.identity.name, r[0].name)

        for partition in self.bundle.partitions:
            r = api.find((QueryCommand().partition(name = partition.identity.name)).to_dict())
            self.assertEquals(partition.identity.name, r[0].name)
    def test_joint_resolve(self):
        '''Test resolving from either a remote or local library, from the local interface '''

        from ambry.identity import Identity
        from ambry.client.rest import RemoteLibrary
        from ambry.library.query import RemoteResolver

        self.start_server()

        config = self.server_library_config()

        # Create the library so we can get the same remote config
        l = new_library(config)

        s3 = l.upstream.last_upstream()
        print l.info
        db = l.database
        db.enable_delete = True
        db.clean()

        l.put_bundle(self.bundle)

        # This might not do anything if the files already are in s3
        def push_cb(action, metadata, time):
            print action, metadata['fqname']

        l.push(cb=push_cb)

        # Check they are on the web
        self.web_exists(s3,self.bundle.identity.cache_key)
        for p in self.bundle.partitions:
            self.web_exists(s3,p.identity.cache_key)

        # Check the basic resolvers
        ident = self.bundle.identity
        self.assertEquals(ident.vid, l.resolve(ident.vid).vid)

        rl = RemoteLibrary(self.server_url)
        self.assertEquals(ident.vid, rl.resolve(ident.vname).vid)

        # That's the basics, now test the primary use case with the remote resolver.

        # Remote resolver only
        rr = RemoteResolver(local_resolver=None, remote_urls=[self.server_url])
        self.assertEquals(ident.vid, rr.resolve_ref_one(ident.vid)[1].vid)
        self.assertEquals('http://localhost:7979', rr.resolve_ref_one(ident.vid)[1].url)

        # Local Resolver only
        rr = RemoteResolver(local_resolver=l.database.resolver, remote_urls=None)
        self.assertEquals(ident.vid, rr.resolve_ref_one(ident.vid)[1].vid)
        self.assertIsNone(rr.resolve_ref_one(ident.vid)[1].url)

        self.stop_server()

        # Remote resolver only
        rr = RemoteResolver(local_resolver=None, remote_urls=[self.server_url])
        self.assertIsNone(rr.resolve_ref_one(ident.vid)[1])

        # Combined
        rr = RemoteResolver(local_resolver=l.database.resolver, remote_urls=[self.server_url])
        self.assertEquals(ident.vid, rr.resolve_ref_one(ident.vid)[1].vid)
        self.assertIsNone(rr.resolve_ref_one(ident.vid)[1].url)

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

    def test_library_get(self):
        from ambry.library import new_library

        vid = self.bundle.identity.vid

        config = self.server_library_config()

        # Create the library so we can get the same remote config
        server_l = new_library(config)

        server_l.put_bundle(self.bundle)

        # Local only; no connection to server
        local_l  = new_library(self.server_rc.library("local"))

        remote_l = new_library(self.server_rc.library("reader"))
        remote_l.purge()

        self.assertTrue(len(remote_l.list()) == 0)

        self.assertEquals(vid, server_l.resolve(vid).vid)
        self.assertIsNone(local_l.resolve(vid))
        self.assertIsNone(remote_l.resolve(vid))

        self.start_server()

        self.assertEquals(vid, remote_l.resolve(vid).vid)

        b = remote_l.get(vid)

        print b.identity.fqname

        for p in self.bundle.partitions:
            b = remote_l.get(p.identity.vid)
            self.assertTrue(p.identity.fqname, b.partition.identity.fqname)

        self.assertEqual(1, len(remote_l.list()))


    # =======================

    def x_test_remote_library(self):
   
        # This test does not work with the threaded test server. 
        
        # It does work with an external server, but you have to delete 
        # All of the files on the remote library between runs. 
   
        #
        # First store the files in the local library
        #
        
        self.start_server()
        
        self.get_library('server').purge()
        self.get_library('clean').purge()

        l = self.get_library()
     
        r = l.put(self.bundle)

        r = l.get(self.bundle.identity.name)
        self.assertEquals(self.bundle.identity.name, r.identity.name)

        for partition in self.bundle.partitions:
            r = l.put(partition)

            # Get the partition with a name
            r = l.get(partition.identity.name)
            self.assertTrue(bool(r))
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.identity.name)
            
            # Get the partition with an id
            r = l.get(partition.identity.id_)
            self.assertTrue(bool(r))
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.identity.name)            

        #
        # Now start with a different, clean library with the same remote
        #

        # haven't pushed yet, so should fail. 
        l2 = self.get_library('clean')
        b = l2.get(self.bundle.identity.name)
        self.assertTrue(not b)
        
        # Copy all of the newly added files to the server. 
        l.push()
   
        l2 = self.get_library('clean')

        r = l2.get(self.bundle.identity.name)

        self.assertTrue(bool(r))

        r = l2.get(r.partitions.all[0].identity.id_)

        self.assertTrue(bool(r))
        self.assertTrue(os.path.exists(r.partition.database.path))

    def x_test_remote_library_partitions(self):

        self.start_server()

        l = self.get_library()
     
        r = l.put(self.bundle)

        r = l.get(self.bundle.identity.name)
        self.assertEquals(self.bundle.identity.name, r.identity.name)

        for partition in self.bundle.partitions:
            r = l.put(partition)

            # Get the partition with a name
            r = l.get(partition.identity.name)
            self.assertTrue(r is not False)
            self.assertEquals(partition.identity.name, r.partition.identity.name)
            self.assertEquals(self.bundle.identity.name, r.identity.name)

        # Copy all of the newly added files to the server. 
        l.push()
            
        l2 = new_library('clean')
        l2.purge()
        
        r = l2.get('b1DxuZ001')
     
        self.assertTrue(r is not None and r is not False)
        
        print r
        
        self.assertTrue(r.partition is not None and r.partition is not False)
        self.assertEquals(r.partition.identity.id_,'b1DxuZ001' )
        
        self.assertTrue(os.path.exists(r.partition.database.path))
   
    def x_test_test(self):
        from ambry.client.siesta import  API
        
        self.start_server()
        
        a = API(self.server_url)
        
        # Test echo for get. 
        r = a.test.echo('foobar').get(bar='baz')
        
        self.assertEquals(200,r.status)
        self.assertIsNone(r.exception)
        
        self.assertEquals('foobar',r.object[0])
        self.assertEquals('baz',r.object[1]['bar'])
        
        # Test echo for put. 
        r = a.test.echo().put(['foobar'],bar='baz')
        
        self.assertEquals(200,r.status)
        self.assertIsNone(r.exception)

        self.assertEquals('foobar',r.object[0][0])
        self.assertEquals('baz',r.object[1]['bar'])
      
        
        with self.assertRaises(Exception):
            r = a.test.exception.put('foo')
        
        with self.assertRaises(Exception):
            r = a.test.exception.get()

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

    def x_test_put_bundle_noremote(self):
        return self._test_put_bundle('default')

    def x_test_put_bundle_remote(self):
        return self._test_put_bundle('default-remote', self.rc.accounts)

    def x_test_remote_cache(self):
        self.start_server(name='default-remote')
    
    def x_test_put_redirect(self):
        from ambry.bundle import DbBundle
        from ambry.library.query import QueryCommand
        from ambry.util import md5_for_file, rm_rf, bundle_file_type

        #
        # Simple out and retrieve
        # 
        cache = self.bundle.filesystem._get_cache(self.server_rc.filesystem, 'direct-remote')
        cache2 = self.bundle.filesystem._get_cache(self.server_rc.filesystem, 'direct-remote-2')

        rm_rf(os.path.dirname(cache.cache_dir))
        rm_rf(os.path.dirname(cache2.cache_dir))
        
        cache.put( self.bundle.database.path, 'direct')

        path = cache2.get('direct')

        self.assertEquals('sqlite',bundle_file_type(path))

        cache.remove('direct', propagate = True)

        #
        #  Connect through server. 
        #
        rm_rf('/tmp/server')
        self.start_server(name='default-remote')
        
        api = None # Rest(self.server_url, self.rc.accounts)  

        # Upload directly, then download via the cache. 
        
        cache.remove(self.bundle.identity.cache_key, propagate = True)
        
        r = api.upload_file(self.bundle.identity, self.bundle.database.path, force=True )

        path = cache.get(self.bundle.identity.cache_key)
        
        b = DbBundle(path)

        self.assertEquals("source-dataset-subset-variation-ca0d",b.identity.name )
      
        #
        # Full service
        #

        p  = self.bundle.partitions.all[0]

        cache.remove(self.bundle.identity.cache_key, propagate = True)
        cache.remove(p.identity.cache_key, propagate = True)
        
        r = api.put( self.bundle.database.path, self.bundle.identity )
        print "Put {}".format(r.object)
        r = api.put(p.database.path, p.identity )
        print "Put {}".format(r.object)
        
        r = api.put(p.database.path, p.identity )
        
        r = api.get(p.identity,'/tmp/foo.db')
        print "Get {}".format(r)        

        b = DbBundle(r)

        self.assertEquals("source-dataset-subset-variation-ca0d",b.identity.name )

    def x_test_dump(self):
        import time
        import logging 
     
       
        l = new_library(self.server_rc.library('default-remote'), reset = True)
        l.clean()

        self.start_server()
        
        l.run_dumper_thread()
        l.run_dumper_thread()
       
        self.assertFalse(l.database.needs_dump())
        l.put(self.bundle)
        self.assertTrue(l.database.needs_dump()) 
        l.run_dumper_thread()
        time.sleep(6)
        self.assertFalse(l.database.needs_dump())
            
        l.run_dumper_thread()
        l.put(self.bundle)
        l.run_dumper_thread()
        time.sleep(7)
        print l.database.needs_dump()
        self.assertFalse(l.database.needs_dump())
        
        self.assertEquals(self.bundle.identity.name,  l.get(self.bundle.identity.name).identity.name)
        
        l.clean()
        
        self.assertEqual(None, l.get(self.bundle.identity.name))
        
        l.restore()
        
        self.assertEquals(self.bundle.identity.name,  l.get(self.bundle.identity.name).identity.name)
        
        

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())