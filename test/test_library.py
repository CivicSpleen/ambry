'''
Created on Jun 30, 2012

@author: eric
'''
import unittest
import os.path
from  bundles.testbundle.bundle import Bundle
from ambry.run import  get_runconfig, RunConfig
from ambry.library.query import QueryCommand
import logging
import ambry.util

from test_base import  TestBase

global_logger = ambry.util.get_logger(__name__)
global_logger.setLevel(logging.DEBUG)

class Test(TestBase):
 
    def setUp(self):
        import bundles.testbundle.bundle



        self.bundle_dir = os.path.dirname(bundles.testbundle.bundle.__file__)
        self.rc = get_runconfig((os.path.join(self.bundle_dir,'library-test-config.yaml'),
                                 os.path.join(self.bundle_dir,'bundle.yaml'),
                                 RunConfig.USER_ACCOUNTS)
                                 )

        self.copy_or_build_bundle()

        self.bundle = Bundle()    

        print "Deleting: {}".format(self.rc.group('filesystem').root)
        Test.rm_rf(self.rc.group('filesystem').root)
       
    @staticmethod
    def rm_rf(d):
        
        if not os.path.exists(d):
            return
        
        for path in (os.path.join(d,f) for f in os.listdir(d)):
            if os.path.isdir(path):
                Test.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)


    def get_library(self, name = 'default'):
        """Clear out the database before the test run"""
        from ambry.library import new_library

        config = self.rc.library(name)

        l =  new_library(config, reset = True)

        return l
        

    def tearDown(self):
        pass


    @staticmethod
    def new_db():
        from ambry.util import temp_file_name
        from ambry.library.database import LibraryDb
        db_file = temp_file_name()+".db"

        db = LibraryDb(driver='sqlite', dbname=db_file)

        return db_file, db

    def test_database(self):

        f,db = self.new_db()

        ##
        ## Test basic creation
        ##

        self.assertFalse(db.exists())

        db.create()

        self.assertTrue(db.exists())

        db.set_config_value('test','one',1)
        db.set_config_value('test','two',2)

        self.assertEquals(1,db.get_config_value('test','one').value)
        self.assertEquals(2,db.get_config_value('test','two').value)

        self.assertIn(('test', 'one'),db.config_values)
        self.assertIn(('test', 'two'),db.config_values)
        self.assertEquals(2,db.config_values[('test', 'two')])

        self.assertEquals(0, len(db.list()))

        db.drop()

        self.assertTrue(os.path.exists(f))
        self.assertFalse(db.exists())

        os.remove(f)


    def test_database_query(self):
        from ambry.orm import Dataset, Partition
        from ambry.library.query import Resolver
        from ambry.library.database import ROOT_CONFIG_NAME_V

        f,db = self.new_db()
        print 'Testing ', f
        db.create()

        db.install_bundle(self.bundle)

        #
        # Get a bunch of names from the existing bundles. This will check the simple
        # queries for single objects.
        #

        tests = {}
        for r in db.session.query(Dataset, Partition).filter(Dataset.vid != ROOT_CONFIG_NAME_V).all():

            di = r.Dataset.identity

            tests[di.sname] = di.vid
            tests[di.vname] = di.vid
            tests[di.fqname] = di.vid
            tests[di.vid] = di.vid

            pi = r.Partition.identity

            tests[pi.sname] = pi.vid
            tests[pi.vname] = pi.vid
            tests[pi.fqname] = pi.vid
            tests[pi.vid] = pi.vid

        r = Resolver(db.session)

        for ref, vid in tests.items():

            ip, results = r.resolve_ref_all(ref)

            self.assertEqual(1, len(results))

            first= results.values().pop(0)
            vid2 = first.vid if not first.partitions  else first.partitions.values()[0].vid

            self.assertEquals(vid, vid2)


    def test_simple_install(self):

        from ambry.util import temp_file_name
        import pprint
        import os
        
        l = self.get_library()
        print "Library: ", l.database.dsn
     
        r = l.put_bundle(self.bundle)

        r = l.get(self.bundle.identity.sname)
        self.assertTrue(r is not False)
        self.assertEquals(self.bundle.identity.sname, r.identity.sname)

        r = l.get('dibberish')
        self.assertFalse(r)

        for partition in self.bundle.partitions:
            print "Install and check: ", partition.identity.vname
            r = l.put_partition(self.bundle, partition)

            # Get the partition with a name
            r = l.get(partition.identity.sname)
            self.assertTrue(r is not False)
            self.assertEquals(partition.identity.sname, r.partition.identity.sname)
            self.assertEquals(self.bundle.identity.sname, r.identity.sname)
            
            # Get the partition with an id
            r = l.get(partition.identity.id_)

            self.assertTrue(bool(r))
            self.assertEquals(partition.identity.sname, r.partition.identity.sname)
            self.assertEquals(self.bundle.identity.sname, r.identity.sname)

        self.assertTrue(l.database.needs_dump())

        backup_file = temp_file_name()+".db"
        
        l.database.dump(backup_file)
        
        #l.database.close()
        os.remove(l.database.dbname)
        l.database.create()

        r = l.get(self.bundle.identity.sname)
    
        self.assertTrue(not r)
        
        l.database.restore(backup_file)
        
        r = l.get(self.bundle.identity.sname)
        self.assertTrue(r is not False)
        self.assertEquals(self.bundle.identity.sname, r.identity.sname)

        os.remove(backup_file)

        # An extra change so the following tests work
        l.put_bundle(self.bundle)
        
        self.assertFalse(l.database.needs_dump())

        import time; time.sleep(10)

        self.assertTrue(l.database.needs_dump())
      

    def test_library_install(self):
        '''Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent'''

        l = self.get_library()

        print l.database.dsn

        l.put_bundle(self.bundle)
        l.put_bundle(self.bundle)

        r = l.get(self.bundle.identity)

        self.assertIsNotNone(r)
        self.assertTrue(r is not False)
        self.assertEquals(r.identity.id_, r.identity.id_)

        # Install the partition, then check that we can fetch it
        # a few different ways.
        for partition in self.bundle.partitions:
            l.put_partition(self.bundle, partition)
            l.put_partition(self.bundle, partition)

            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals( partition.identity.id_, r.partition.identity.id_)
            
            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(partition.identity.id_, r.partition.identity.id_)
            
        # Re-install the bundle, then check that the partitions are still properly installed

        l.put_bundle(self.bundle)
        
        for partition in self.bundle.partitions.all:
       
            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals(r.partition.identity.id_, partition.identity.id_)
            
            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(r.partition.identity.id_, partition.identity.id_)
            
        # Find the bundle and partitions in the library. 
    
        r = l.find(QueryCommand().table(name='tone'))

        self.assertEquals('source-dataset-subset-variation',r[0]['identity']['name'])
    
        r = l.find(QueryCommand().table(name='tone').partition(format='db', grain=None))

        self.assertEquals('source-dataset-subset-variation-tone',r[0]['partition']['name'])
        
        r = l.find(QueryCommand().table(name='tthree').partition(format='db', segment=None))
        self.assertEquals('source-dataset-subset-variation-tthree',r[0]['partition']['name'])

        #
        #  Try getting the files 
        # 
        
        r = l.find(QueryCommand().table(name='tthree').partition(any=True)) #@UnusedVariable
       
        bp = l.get(r[0]['identity']['id'])
        
        self.assertTrue(os.path.exists(bp.database.path))
        
        # Put the bundle with remove to check that the partitions are reset
        
        l.remove(self.bundle)
        
        r = l.find(QueryCommand().table(name='tone').partition(any=True))
        self.assertEquals(0, len(r))      
        
        l.put_bundle(self.bundle)
    
        r = l.find(QueryCommand().table(name='tone').partition(any=True))
        self.assertEquals(2, len(r))
       
        ds_names = [ds.sname for ds in l.list().values()]
        self.assertIn('source-dataset-subset-variation', ds_names)


    def test_library_push(self):
        '''Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent'''
        from ambry.bundle import DbBundle

        l = self.get_library('local-remoted')

        print l.info

        l.put_bundle(self.bundle)

        r = l.get(self.bundle.identity)

        self.assertIsNotNone(r)
        self.assertTrue(r is not False)
        self.assertEquals(r.identity.id_, r.identity.id_)

        self.assertEquals(6, len(l.files.query.state('new').all))

        for remote in l.remotes:
            remote.clean()

        a = l.remotes
        b = l.files.query.state('new').all

        def cb(what, metadata, start):
            print "PUSH ", what, metadata['name'], start

        # The zippy bit rotates the files through the three caches.
        for remote, file_ in zip(a*(len(b)/len(a)+1),b):
            l.push(file_.ref, upstream=remote, cb=cb)

        import yaml
        self.assertEqual(
"""source/dataset-subset-variation-0.0.1.db:
  caches: [/tmp/library-test/remote-cache-1]
source/dataset-subset-variation-0.0.1/geot1.geodb:
  caches: [/tmp/library-test/remote-cache-2]
source/dataset-subset-variation-0.0.1/geot2.geodb:
  caches: [/tmp/library-test/remote-cache-3]
source/dataset-subset-variation-0.0.1/tone.db:
  caches: [/tmp/library-test/remote-cache-3]
source/dataset-subset-variation-0.0.1/tone/missing.db:
  caches: [/tmp/library-test/remote-cache-2]
source/dataset-subset-variation-0.0.1/tthree.db:
  caches: [/tmp/library-test/remote-cache-1]
""",
        yaml.safe_dump(l.remote_stack.list(include_partitions=True)))

        l.purge()

        fn = l.remote_stack.get(self.bundle.identity.cache_key)

        self.assertTrue(bool(fn))
        self.assertEquals('/tmp/library-test/remote-cache-1/source/dataset-subset-variation-0.0.1.db', fn)

        c = l.cache

        self.assertNotIn('source/dataset-subset-variation-0.0.1.db', c.list())
        c.attach(l.remote_stack)
        self.assertIn('source/dataset-subset-variation-0.0.1.db', c.list())
        c.detach()
        self.assertNotIn('source/dataset-subset-variation-0.0.1.db', c.list())

        l.sync_remotes()

        b = l.get(self.bundle.identity.vid)

        self.assertIsNotNone(b)

        print b.identity

        for p in b.partitions:
            bp = l.get(p.identity.vid)

            print '----'
            print bp.identity.vname
            print bp.partition.identity.vname


    def test_s3_push(self):
        '''Install the bundle and partitions, and check that they are
        correctly installed. Check that installation is idempotent'''
        from ambry.cache.remote import  HttpCache

        root = self.rc.group('filesystem').root

        l = self.get_library('s3-remoted')

        print l.info

        l.purge()
        l.put_bundle(self.bundle)

        def cb(what, metadata, start):
            print "PUSH ", what, metadata['name'], start

        l.push(cb=cb)

        l.purge()

        l.sync_remotes()

        b = l.get(self.bundle.identity.vid)

        self.assertTrue(bool(b))

        self.assertEquals(self.bundle.identity.vname, b.identity.vname)

        for p in b.partitions:
            bp = l.get(p.identity.vid)

            self.assertEquals(self.bundle.identity.vname, bp.identity.vname)
            self.assertEquals(p.identity.vname, bp.partition.vname)

            file_ = l.files.query.ref(bp.partition.identity.vid).one


        dataset = l.resolve(p.identity.vid)


        self.assertEquals('piEGPXmDC8005001', dataset.partition.vid)

        l.remotes[0].store_list()



    def test_http_cache(self):

        l = self.get_library('http-remoted')


        r = l.remotes[0]


        self.assertFalse(r.has('foobar'))
        self.assertTrue(r.has(self.bundle.identity.cache_key))

        l.purge()

        l.sync_remotes()

        b = l.get(self.bundle.identity.cache_key)

        self.assertTrue(bool(b))

        self.assertEquals(self.bundle.identity.vname, b.identity.vname)

        for p in b.partitions:
            bp = l.get(p.identity.vid)
            print p.identity.vname
            self.assertEquals(self.bundle.identity.vname, bp.identity.vname)
            self.assertEquals(p.identity.vname, bp.partition.vname)



    def test_versions(self):
        import bundles.testbundle.bundle
        from ambry.run import get_runconfig
        from ambry.library.query import Resolver
        import shutil
        idnt = self.bundle.identity

        l = self.get_library()

        l.purge()

        orig = os.path.join(self.bundle.bundle_dir,'bundle.yaml')
        save = os.path.join(self.bundle.bundle_dir,'bundle.yaml.save')
        shutil.copyfile(orig,save)

        datasets = {}

        try:
            for i in [1,2,3]:
                idnt._on.revision = i
                idnt.name.version_major = i
                idnt.name.version_minor = i*10

                bundle = Bundle()
                get_runconfig.clear() #clear runconfig cache

                bundle.metadata.load_all()

                bundle.metadata.identity = idnt.ident_dict
                bundle.metadata.names = idnt.names_dict

                bundle.metadata.write_to_dir(write_all=True)

                print 'Building version {}'.format(i)

                bundle = Bundle()

                bundle.clean()
                bundle.pre_prepare()
                bundle.prepare()
                bundle.post_prepare()
                bundle.pre_build()
                bundle.build_small()
                #bundle.build()
                bundle.post_build()

                bundle = Bundle()

                print "Installing ", bundle.identity.vname
                l.put_bundle(bundle)

        finally:
            pass
            os.rename(save, orig)

        #
        # Save the list of datasets for version analysis in other
        # tests
        #

        db = l.database

        for d in db.list(with_partitions=True).values():
            datasets[d.vid] = d.dict
            datasets[d.vid]['partitions'] = {}

            for p_vid, p in d.partitions.items():
                datasets[d.vid]['partitions'][p_vid] = p.dict


        with open(self.bundle.filesystem.path('meta','version_datasets.json'),'w') as f:
            import json
            f.write(json.dumps(datasets))


        r = Resolver(db.session)

        ref = idnt.id_

        ref = "source-dataset-subset-variation-=2.20"

        ip, results = r.resolve_ref_all(ref)

        for row in results:
            print row


        #os.remove(f)


    def test_version_resolver(self):
        from ambry.library.query import Resolver

        l = self.get_library()
        print l.database.dsn


        db = l.database
        db.enable_delete = True
        db.drop()
        db.create()

        l.put_bundle(self.bundle)


        r = Resolver(db.session)

        vname = 'source-dataset-subset-variation-0.0.1'
        name = 'source-dataset-subset-variation'

        ip, results = r.resolve_ref_one(vname)
        self.assertEquals(vname, results.vname)

        ip, results = r.resolve_ref_one(name)
        self.assertEquals(vname, results.vname)

        # Cache keys

        ip, result = r.resolve_ref_one('source/dataset-subset-variation-0.0.1.db')
        self.assertEquals('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',str(result))

        ip, result = r.resolve_ref_one('source/dataset-subset-variation-0.0.1/tthree.db')
        self.assertEquals('source-dataset-subset-variation-tthree-0.0.1~piEGPXmDC8003001',str(result.partition))


        # Now in the library, which has a slightly different interface.

        ident = l.resolve(vname)
        self.assertEquals(vname, ident.vname)

        ident = l.resolve('source-dataset-subset-variation-0.0.1~diEGPXmDC8001')
        self.assertEquals('diEGPXmDC8001', ident.vid)

        ident = l.resolve('source-dataset-subset-variation-tthree-0.0.1~piEGPXmDC8001001')
        self.assertEquals('diEGPXmDC8001', ident.vid)
        self.assertEquals('piEGPXmDC8001001', ident.partition.vid)

        ##
        ## Test semantic version matching
        ## WARNING! The Mock object below only works for testing semantic versions.
        ##

        with open(self.bundle.filesystem.path('meta','version_datasets.json')) as f:
            import json
            datasets = json.loads(f.read())

        # This mock object only works on datasets; it will return all of the
        # partitions for each dataset, and each of the datasets. It is only for testing
        # version filtering.
        class TestResolver(Resolver):
            def _resolve_ref(self, ref, location=None):
                from ambry.identity import Identity
                ip = Identity.classify(ref)
                return ip, { k:Identity.from_dict(ds) for k,ds in datasets.items() }

        r = TestResolver(db.session)


        ip, result = r.resolve_ref_one('source-dataset-subset-variation-==1.10.1')
        self.assertEquals('source-dataset-subset-variation-1.10.1~diEGPXmDC8001',str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation->=1.10.1,<3.0.0')
        self.assertEquals('source-dataset-subset-variation-2.20.2~diEGPXmDC8002',str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation->=1.10.1,<2.0.0')
        self.assertEquals('source-dataset-subset-variation-1.10.1~diEGPXmDC8001',str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation->2.0.0')
        self.assertEquals('source-dataset-subset-variation-3.30.3~diEGPXmDC8003',str(result))

        ip, result = r.resolve_ref_one('source-dataset-subset-variation-<=3.0.0')
        self.assertEquals('source-dataset-subset-variation-2.20.2~diEGPXmDC8002',str(result))


    def x_test_remote(self):
        from ambry.run import RunConfig
        from ambry.library import new_library
        
        rc = get_runconfig((os.path.join(self.bundle_dir,'server-test-config.yaml'),RunConfig.USER_CONFIG))

        config = rc.library('default')
        library =  new_library(config)

        print library.upstream
        print library.upstream.last_upstream()
        print library.cache
        print library.cache.last_upstream()  


    def test_compression_cache(self):
        '''Test a two-level cache where the upstream compresses files '''
        from ambry.cache.filesystem import  FsCache,FsCompressionCache
         
        root = self.rc.group('filesystem').root
      
        l1_repo_dir = os.path.join(root,'comp-repo-l1')
        os.makedirs(l1_repo_dir)
        l2_repo_dir = os.path.join(root,'comp-repo-l2')
        os.makedirs(l2_repo_dir)
        
        testfile = self.new_rand_file(os.path.join(root,'testfile'))



        # Create a cache with an upstream wrapped in compression
        l3 = FsCache(l2_repo_dir)
        l2 = FsCompressionCache(l3)
        l1 = FsCache(l1_repo_dir, upstream=l2)
      
        f1 = l1.put(testfile,'tf1')         
  
        self.assertTrue(os.path.exists(f1))  
        
        l1.remove('tf1', propagate=False)
        
        self.assertFalse(os.path.exists(f1))  
        
        f1 = l1.get('tf1')
        
        self.assertIsNotNone(f1)
        
        self.assertTrue(os.path.exists(f1))  
        

    def test_partitions(self):
        from ambry.identity import PartitionNameQuery
        from sqlalchemy.exc import IntegrityError
        
        l = self.get_library()
        print l.info

        l.purge()
         

    
    
        l.put_bundle(self.bundle) # Install the partition references in the library.

        b = l.get(self.bundle.identity)

        for partition in self.bundle.partitions:

            l.put_partition(self.bundle, partition)
            l.put_partition(self.bundle, partition)

            print partition.identity.sname, partition.database.path

            r = l.get(partition.identity)
            self.assertIsNotNone(r)
            self.assertEquals( partition.identity.id_, r.partition.identity.id_)
            
            r = l.get(partition.identity.id_)
            self.assertIsNotNone(r)
            self.assertEquals(partition.identity.id_, r.partition.identity.id_)


        #
        # Create all possible combinations of partition names
        #
        s = set()
        table = self.bundle.schema.tables[0]

        p = (('time', 'time2'), ('space', 'space3'), ('grain', 'grain4'))
        p += p
        pids = {}
        for i in range(4):
            for j in range(4):
                pid = self.bundle.identity.as_partition(**dict(p[i:i + j + 1]))
                pids[pid.fqname] = pid

        for pid in pids.values():
            print pid.sname
            try:
                # One will fail with an integrity error, but it doesn't matter for this test.

                part = self.bundle.partitions.new_db_partition(**pid.dict)
                part.create()

                parts = self.bundle.partitions._find_orm(PartitionNameQuery(vid=pid.vid)).all()
                self.assertIn(pid.sname, [p.name for p in parts])
            except IntegrityError:
                pass

        
    def test_s3(self):

        #ambry.util.get_logger('ambry.filesystem').setLevel(logging.DEBUG)
        # Set up the test directory and make some test files. 
        from ambry.cache import new_cache
        
        root = self.rc.group('filesystem').root
        os.makedirs(root)
                
        testfile = os.path.join(root,'testfile')
        
        with open(testfile,'w+') as f:
            for i in range(1024):
                f.write('.'*1023)
                f.write('\n')
         
        #fs = self.bundle.filesystem
        #local = fs.get_cache('downloads')
        
        cache = new_cache(self.rc.filesystem('s3'))
        repo_dir  = cache.cache_dir
      
        print "Repo Dir: {}".format(repo_dir)
      
        for i in range(0,10):
            global_logger.info("Putting "+str(i))
            cache.put(testfile,'many'+str(i))
        
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many1')))   
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many2')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))
                
        p = cache.get('many1')
        self.assertTrue(p is not None)
                
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many1')))   
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many2')))
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))
        
        p = cache.get('many2')
        self.assertTrue(p is not None)
                
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many3')))      
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many7'))) 
 
        p = cache.get('many3')
        self.assertTrue(p is not None)
                
        self.assertTrue(os.path.exists(os.path.join(repo_dir, 'many3')))      
        self.assertFalse(os.path.exists(os.path.join(repo_dir, 'many7'))) 


    def test_query(self):
        from ambry.library.query import QueryCommand
        
        tests = [
            "column.name = 'column.name', identity.id='identity',",
            "column.name = 'column.name', identity.id='identity' ",
            "column.name = 'column.name' identity.id = 'identity'",
            "partition.vname ='partition.vname'",
            "partition.vname = '%partition.vname%'",
            "identity.name = '%clarinova foo bar%'"
            
            ]
        
        fails = [
            "column.name='foobar"
        ]
        
        for s in tests:
            qc = QueryCommand.parse(s)
            print qc
    
        for s in fails:

            self.assertRaises(QueryCommand.ParseError, QueryCommand.parse, s)


    def test_files(self):

        l = self.get_library()

        l.purge()

        fls = l.files

        for e in [ (str(i), str(j) ) for i in range(10) for j in range(3)  ]:

            f = l.files.new_file(path='path'+e[0], ref="{}-{}".format(*e), group=e[1], type_=e[1])

            l.files.merge(f)

        def refs(itr):
            return [ f.ref for f in i ]

        print l.files.query.path('path3').first

    def test_ilibrary(self):

        import ambry

        l = ambry.ilibrary()
        print l.find(name="clarinova.com")


    def test_adhoc(self):
        from ambry.library import new_library

        config = get_runconfig().library('default')

        l = new_library(config, reset=True)

        print l.resolve('211sandiego.org-calls-p1ye2014-orig-calls')

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())