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

        rm_rf('/tmp/cache-test')

        self.copy_or_build_bundle()

        self.bundle_dir = os.path.dirname(bundles.testbundle.bundle.__file__)

        self.rc = get_runconfig((os.path.join(self.bundle_dir, 'test-run-config.yaml'),
                            RunConfig.USER_ACCOUNTS))

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

        config = self.rc.library(name)

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

    def make_test_file(self):
        root = self.rc.group('filesystem').root

        testfile = os.path.join(root, 'testfile')

        if not os.path.exists(root):
            os.makedirs(root)

        with open(testfile, 'w+') as f:
            for i in range(1024):
                f.write('.' * 1023)
                f.write('\n')

        return testfile

    def test_basic(self):

        from ambry.cache.filesystem import FsCache, FsLimitedCache

        root = self.rc.group('filesystem').root

        l1_repo_dir = os.path.join(root, 'repo-l1')
        os.makedirs(l1_repo_dir)
        l2_repo_dir = os.path.join(root, 'repo-l2')
        os.makedirs(l2_repo_dir)

        testfile = self.make_test_file()
        #
        # Basic operations on a cache with no upstream
        #
        l2 = FsCache(l2_repo_dir)

        p = l2.put(testfile, 'tf1')
        l2.put(testfile, 'tf2')
        g = l2.get('tf1')

        self.assertTrue(os.path.exists(p))
        self.assertTrue(os.path.exists(g))
        self.assertEqual(p, g)

        self.assertIsNone(l2.get('foobar'))

        l2.remove('tf1')

        self.assertIsNone(l2.get('tf1'))

        #
        # Now create the cache with an upstream, the first
        # cache we created

        l1 = FsLimitedCache(l1_repo_dir, upstream=l2, size=5)

        print l1
        print l2

        g = l1.get('tf2')
        self.assertTrue(g is not None)

        # Put to one and check in the other.

        l1.put(testfile, 'write-through')
        self.assertIsNotNone(l2.get('write-through'))

        l1.remove('write-through', propagate=True)
        self.assertIsNone(l2.get('write-through'))

        # Put a bunch of files in, and check that
        # l2 gets all of the files, but the size of l1 says constrained
        for i in range(0, 10):
            l1.put(testfile, 'many' + str(i))

        self.assertEquals(4194304, l1.size)


        # Check that the right files got deleted
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many1')))
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many5')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many6')))

        # Fetch a file that was displaced, to check that it gets loaded back
        # into the cache.
        p = l1.get('many1')
        p = l1.get('many2')
        self.assertTrue(p is not None)
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many1')))
        # Should have deleted many6
        self.assertFalse(os.path.exists(os.path.join(l1.cache_dir, 'many6')))
        self.assertTrue(os.path.exists(os.path.join(l1.cache_dir, 'many7')))

        #
        # Check that verification works
        #
        l1.verify()

        os.remove(os.path.join(l1.cache_dir, 'many8'))

        with self.assertRaises(Exception):
            l1.verify()

        l1.remove('many8')

        l1.verify()

        c = l1.database.cursor()
        c.execute("DELETE FROM  files WHERE path = ?", ('many9',))
        l1.database.commit()

        with self.assertRaises(Exception):
            l1.verify()

        l1.remove('many9')

        l1.verify()

    def test_basic_prefix(self):
        from ambry.cache.filesystem import FsCache, FsLimitedCache

        root = self.rc.group('filesystem').root

        cache_dir = os.path.join(root, 'test_prefixes')

        prefix = 'prefix'

        cache1 = FsCache(cache_dir)

        cache2 = FsCache(cache_dir, prefix = prefix)

        tf = self.make_test_file()

        cache2.put(tf, 'tf')

        self.assertTrue(cache1.has('{}/{}'.format(prefix,'tf')))

    def test_compression(self):
        from ambry.run import get_runconfig
        from ambry.cache import new_cache
        from ambry.util import temp_file_name, md5_for_file, copy_file_or_flo

        rc = get_runconfig((os.path.join(self.bundle_dir, 'test-run-config.yaml'), RunConfig.USER_CONFIG))

        comp_cache = new_cache(rc.filesystem('compressioncache'))

        test_file_name = 'test_file'

        fn = temp_file_name()
        print 'orig file ', fn
        with open(fn, 'wb') as f:
            for i in range(1000):
                f.write("{:03d}:".format(i))

        cf = comp_cache.put(fn, test_file_name)

        with open(cf) as stream:
            from ambry.util.sgzip import GzipFile

            stream = GzipFile(stream)

            uncomp_cache = new_cache(rc.filesystem('fscache'))

            uncomp_stream = uncomp_cache.put_stream('decomp')

            copy_file_or_flo(stream, uncomp_stream)

        uncomp_stream.close()

        dcf = uncomp_cache.get('decomp')

        self.assertEquals(md5_for_file(fn), md5_for_file(dcf))

        os.remove(fn)

    def test_md5(self):
        from ambry.run import get_runconfig
        from ambry.cache import new_cache
        from ambry.util import md5_for_file
        from ambry.cache.filesystem import make_metadata

        rc = get_runconfig((os.path.join(self.bundle_dir, 'test-run-config.yaml'), RunConfig.USER_CONFIG))

        fn = self.make_test_file()

        md5 = md5_for_file(fn)

        cache = new_cache(rc.filesystem('fscache'))

        cache.put(fn, 'foo1')

        abs_path = cache.path('foo1')

        self.assertEquals(md5, cache.md5('foo1'))

        cache = new_cache(rc.filesystem('compressioncache'))

        cache.put(fn, 'foo2', metadata=make_metadata(fn))

        abs_path = cache.path('foo2')

        self.assertEquals(md5, cache.md5('foo2'))

        os.remove(fn)

    def test_configed_caches(self):
        '''Basic test of put(), get() and has() for all cache types'''
        from functools import partial
        from ambry.run import  get_runconfig, RunConfig
        from ambry.filesystem import Filesystem
        from ambry.cache import new_cache
        from ambry.util import md5_for_file
        from ambry.bundle import DbBundle

        fn = self.bundle.database.path

        # Opening the file might run the database updates in
        # database.sqlite._on_connect_update_schema, which can affect the md5.
        b = DbBundle(fn)

        md5 = md5_for_file(fn)

        for i, fsname in enumerate(['fscache', 'limitedcache', 'compressioncache',
                                    'cached-s3', 'cached-compressed-s3']):

            config = self.rc.filesystem(fsname)
            cache = new_cache(config)
            print '---', fsname, cache
            identity = self.bundle.identity

            relpath = identity.cache_key

            r = cache.put(fn, relpath,identity.to_meta(md5=md5))

            r = cache.get(relpath)

            if not r.startswith('http'):
                self.assertTrue(os.path.exists(r), 'Not a url: {}: {}'.format(r,str(cache)))

            self.assertTrue(cache.has(relpath, md5=md5))

            clone = cache.clone()

            self.assertTrue(clone.has(relpath, md5=md5), '{}'.format(type(clone)))

            cache.remove(relpath, propagate=True)

            self.assertFalse(os.path.exists(r), str(cache))

            self.assertFalse(cache.has(relpath))
            self.assertFalse(clone.has(relpath))



        cache = new_cache(self.rc.filesystem('s3cache-noupstream'))
        r = cache.put(fn, 'a')

    def test_url_caches(self):
        from ambry.util import rm_rf

        root = self.rc.group('filesystem').root

        rm_rf(root)

        l = self.get_library('remoted')

        print l.info

        r = l.put_bundle(self.bundle)

        def cb(what, metadata, start):
            print "PUSH ", what, metadata['name'], start

        for remote in l.remotes[0:3]:

            # This really should use update(), but it throws inscrutable exceptions.
            for f in l.files.query.state('pushed').all:
                f.state = 'new'
                l.files.merge(f)

            print 'Pushing to ', remote
            l.push(cb=cb, upstream=remote)

        l.purge() # Remove the entries from the library


        l.sync_remotes(remotes=l.remotes[0:3])

        r = l.resolve(self.bundle.identity.vid)

        self.assertEquals('diEGPXmDC8001', str(r.vid))

        r = l.resolve(self.bundle.partitions.all[0].identity.vid)

        self.assertEquals('diEGPXmDC8001', str(r.vid))
        self.assertEquals('piEGPXmDC8001001', str(r.partition.vid))

        r = l.locate(self.bundle.identity.vid)

    def x_test_url_caches_2(self):

        l = self.get_library('remoted')

        ident, r = l.locate(self.bundle.identity.vid)

        print ident

        b = r.get(ident.cache_key)

        print b

        print l.locate_one(self.bundle.identity.vid)
        print l.locate_one('dfoobar')

    def test_attachment(self):
        from ambry.cache import new_cache

        root = self.rc.group('filesystem').root

        rm_rf(root)

        testfile = self.new_rand_file(os.path.join(root, 'testfile'))

        fs1 = new_cache(dict(dir=os.path.join(root,'fs1')))

        fs3 = new_cache(dict(dir=os.path.join(root,'fs3')))

        fs3.put(testfile,'tf')
        self.assertTrue(fs3.has('tf'))
        fs3.remove('tf')
        self.assertFalse(fs3.has('tf'))

        # Show that attachment works, and that deletes propagate.
        fs3.attach(fs1)
        fs3.put(testfile, 'tf')
        self.assertTrue(fs3.has('tf'))
        self.assertTrue(fs1.has('tf'))
        fs3.remove('tf', propagate=True)
        self.assertFalse(fs3.has('tf'))
        self.assertFalse(fs1.has('tf'))
        fs3.detach()

        # Show detachment works
        fs3.attach(fs1)
        fs3.put(testfile, 'tf')
        self.assertTrue(fs3.has('tf'))
        self.assertTrue(fs1.has('tf'))
        fs3.detach()
        fs3.remove('tf', propagate=True)
        self.assertFalse(fs3.has('tf'))
        self.assertTrue(fs1.has('tf'))

    def test_multi_cache(self):
        from ambry.cache import new_cache
        from ambry.cache.multi import MultiCache
        from ambry.bundle import DbBundle

        root = self.rc.group('filesystem').root

        rm_rf(root)

        testfile = self.new_rand_file(os.path.join(root, 'testfile'), size=2)

        fs1 = new_cache(dict(dir=os.path.join(root, 'fs1')))
        fs2 = new_cache(dict(dir=os.path.join(root, 'fs2')))
        fs3 = new_cache(dict(dir=os.path.join(root, 'fs3')))

        caches = [fs1, fs2, fs3]

        for i,cache in enumerate(caches,1):
            cache.put(testfile, 'fs'+str(i), metadata={'i':i})
            j = (i+1) %  len(caches)

            caches[j].put(testfile, 'fs'+str(i), metadata={'i':i})


        mc = MultiCache(caches)
        ls = mc.list()

        self.assertEqual(3, len(ls))
        self.assertIn('fs1',ls)
        self.assertIn('fs3', ls)

        self.assertIn('/tmp/cache-test/fs1', ls['fs1']['caches'])
        self.assertIn('/tmp/cache-test/fs3', ls['fs1']['caches'])

        mc2 = MultiCache([fs1, fs2])
        ls = mc2.list()
        self.assertEqual(3, len(ls))
        self.assertIn('fs1', ls)
        self.assertIn('fs3', ls)


        mc.put(testfile, 'mc1')
        ls = mc.list()
        self.assertEqual(4, len(ls))
        self.assertIn('mc1', ls)

        # Put should have gone to first cache
        mc2 = MultiCache([fs2, fs3])
        ls = mc2.list()
        self.assertEqual(3, len(ls))
        self.assertNotIn('mc1', ls)
        self.assertIn('fs1', ls)
        self.assertIn('fs3', ls)

        l = self.get_library('remoted')

        mc = MultiCache(l.remotes)

        kc = 'source/dataset-subset-variation-0.0.1.db'

        print mc.list()

        self.assertIn(kc, mc.list())

        # Check that has() propagates

        tc = new_cache(dict(dir=os.path.join(root, 'tc1')))

        tc.attach(mc)

        self.assertIn('s3:devtest.sandiegodata.org/cache-compressed',tc.list().values()[0]['caches'])

        self.assertIn(kc, tc.list())

        self.assertTrue(tc.has(kc))

        self.assertFalse(tc.has(kc, propagate = False))

        # Get the object, then check that it persists in the
        # top level cache

        bp = tc.get(kc)

        b = DbBundle(bp)
        self.assertEquals('diEGPXmDC8001', DbBundle(bp).identity.vid )

        self.assertIn('/tmp/cache-test/tc1', tc.list()[kc]['caches'])
        self.assertIn('s3:devtest.sandiegodata.org/cache-compressed', tc.list()[kc]['caches'])

        tc.detach()
        self.assertTrue(tc.has(kc))
        self.assertTrue(tc.has(kc, propagate=False))

        self.assertIn('/tmp/cache-test/tc1', tc.list()[kc]['caches'])
        self.assertNotIn('s3:devtest.sandiegodata.org/cache-compressed', tc.list()[kc]['caches'])

    def test_alt_cache(self):
        from ambry.cache import new_cache
        from ambry.cache.multi import AltReadCache
        from ambry.bundle import DbBundle

        root = self.rc.group('filesystem').root
        print root
        rm_rf(root)

        testfile = self.new_rand_file(os.path.join(root, 'testfile'), size=2)

        fs1 = new_cache(dict(dir=os.path.join(root, 'fs1')))
        fs2 = new_cache(dict(dir=os.path.join(root, 'fs2')))

        fs2.put(testfile, 'fs2', {'foo':'bar'})

        self.assertFalse(fs1.has('fs2'))
        self.assertTrue(fs2.has('fs2'))

        arc = AltReadCache(fs1, fs2)
        self.assertTrue(arc.has('fs2'))
        self.assertEquals(['/tmp/cache-test/fs2'], arc.list()['fs2']['caches'])

        self.assertEquals('/tmp/cache-test/fs1/fs2', arc.get('fs2'))

        # Now the fs1 cache should have the file too
        self.assertTrue(fs1.has('fs2'))
        self.assertTrue(fs2.has('fs2'))

        self.assertIn('foo', fs1.metadata('fs2'))

    def test_http_cache(self):

        from ambry.cache.remote import HttpCache

        c = HttpCache('http://devtest.sandiego')



def suite():


    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())