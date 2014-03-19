"""
Created on Jun 22, 2012

@author: eric
"""
import unittest
from  testbundle.bundle import Bundle
from ambry.identity import *
from test_base import  TestBase

class Test(TestBase):
 
    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()    
        self.bundle_dir = self.bundle.bundle_dir

    def save_bundle(self):
        pass
        
    def restore_bundle(self):
        pass 

    def test_config(self):
        from ambry.bundle.config import BundleFileConfig, RunConfig
        from ambry.util import AttrDict

        import shutil

        p = '/tmp/test_config'
        if not os.path.exists(p):
            os.makedirs(p)

        shutil.copyfile(os.path.join(self.bundle.bundle_dir, 'bundle.yaml'),
                        os.path.join(p, 'bundle.yaml'))


        cfg = BundleFileConfig(p)

        print cfg.build

        cfg.build.foo = 'bar'

        print cfg.build

        cfg.rewrite()

        cfg = RunConfig()

        print cfg.get('build')

    def test_db_bundle(self):
        
        from ambry.bundle import BuildBundle, DbBundle
        
        b = BuildBundle(self.bundle_dir)
        b.clean()
        
        self.assertTrue(b.identity.id_ is not None)
        self.assertEquals('source-dataset-subset-variation', b.identity.sname)
        self.assertEquals('source-dataset-subset-variation-0.0.1', b.identity.vname)
        
        b.database.create()
        
        db_path =  b.database.path
        
        dbb = DbBundle(db_path)
        
        self.assertEqual("source-dataset-subset-variation", dbb.identity.sname)
        self.assertEqual("source-dataset-subset-variation-0.0.1", dbb.identity.vname)

    def test_paths(self):
        ''' Test that a build bundle and a db bundle both produce the same paths. '''
        
        from ambry.bundle import BuildBundle, DbBundle
        
        b = self.bundle
        db  = DbBundle(b.database.path)

        self.assertEqual(b.path, db.path)
        self.assertTrue(os.path.exists(b.path))
        
        self.assertEqual( b.database.path, db.database.path)
        self.assertTrue(os.path.exists(b.database.path))

        self.assertEqual( b.identity.path, db.identity.path)

        for p in zip(b.partitions, db.partitions):
            self.assertTrue(bool(p[0].path))
            self.assertEqual(p[0].path, p[1].path)
            self.assertTrue(bool(p[0].path))
     
    def test_schema_direct(self):
        '''Test adding tables directly to the schema'''
        
        # If we don't explicitly set the id_, it will change for every run. 
        self.bundle.config.identity.id_ = 'aTest'

        self.bundle.schema.clean()

        with self.bundle.session:
            s = self.bundle.schema
            s.add_table('table 1', altname='alt name a')
            s.add_table('table 2', altname='alt name b')
            
            self.assertRaises(Exception,  s.add_table, ('table 1', ))
          
            
            t = s.add_table('table 3', altname='alt name')
        
            s.add_column(t,'col 1',altname='altname1')
            s.add_column(t,'col 2',altname='altname2')
            s.add_column(t,'col 3',altname='altname3')

        
        #print self.bundle.schema.as_csv()
        
        self.assertIn('tiEGPXmDC801', [t.id_ for t in self.bundle.schema.tables])
        self.assertIn('tiEGPXmDC802', [t.id_ for t in self.bundle.schema.tables])
        self.assertNotIn('cTest03', [t.id_ for t in self.bundle.schema.tables])
    
        t = self.bundle.schema.table('table_3')
    
        self.assertIn('ciEGPXmDC803001', [c.id_ for c in t.columns])
        self.assertIn('ciEGPXmDC803002', [c.id_ for c in t.columns])
        self.assertIn('ciEGPXmDC803003', [c.id_ for c in t.columns])
        
        # Try with a nested session, b/c we need to test it somewhere ... 
        with self.bundle.session:
            
            with self.bundle.session:
                
                t = s.add_table('table 4', altname='alt name')
            
                s.add_column(t,'col 1',altname='altname1')
                s.add_column(t,'col 2',altname='altname2')
                s.add_column(t,'col 3',altname='altname3')

        
    def x_test_generate_schema(self):
        '''Uses the generateSchema method in the bundle'''
        from ambry.orm import  Column
        
        with self.bundle.session:
            s = self.bundle.schema
            s.clean()
            
            t1 = s.add_table('table1')
                    
            s.add_column(t1,name='col1', datatype=Column.DATATYPE_REAL )
            s.add_column(t1,name='col2', datatype=Column.DATATYPE_INTEGER )
            s.add_column(t1,name='col3', datatype=Column.DATATYPE_TEXT )  
            
            t2 = s.add_table('table2')   
            s.add_column(t2,name='col1' )
            s.add_column(t2,name='col2' )
            s.add_column(t2,name='col3' )   
    
            t3 = s.add_table('table3') 
            s.add_column(t3,name='col1', datatype=Column.DATATYPE_REAL )
            s.add_column(t3,name='col2', datatype=Column.DATATYPE_INTEGER )
            s.add_column(t3,name='col3', datatype=Column.DATATYPE_TEXT )   

     
    def test_names(self):
        print 'Testing Names'

     
    def test_column_processor(self):
        from ambry.orm import  Column
        from ambry.transform import BasicTransform, CensusTransform
        
        
        
        self.bundle.schema.clean()
        
        with self.bundle.session:
            s = self.bundle.schema  

            t = s.add_table('table3') 
            s.add_column(t,name='col1', datatype=Column.DATATYPE_INTEGER, default=-1, illegal_value = '999' )
            s.add_column(t,name='col2', datatype=Column.DATATYPE_TEXT )   
            s.add_column(t,name='col3', datatype=Column.DATATYPE_REAL )

        
            c1 = t.column('col1')
    
            
            self.assertEquals(1, BasicTransform(c1)({'col1': ' 1 '}))
            
            with self.assertRaises(ValueError):
                print "PROCESSOR '{}'".format(CensusTransform(c1)({'col1': ' B '}))
            
            self.assertEquals(1, CensusTransform(c1)({'col1': ' 1 '}))
            self.assertEquals(-1, CensusTransform(c1)({'col1': ' 999 ' }))
            self.assertEquals(-3, CensusTransform(c1)({'col1': ' # '}))
            self.assertEquals(-2, CensusTransform(c1)({'col1': ' ! '}))
       
       
    def test_validator(self):
       
        #
        # Validators
        #
        
        
        tests =[
                ( 'tone',True, (None,'VALUE',0,0) ),
                ( 'tone',True, (None,'VALUE',-1,0) ),
                ( 'tone',False, (None,'DEFAULT',0,0) ),
                ( 'tone',False, (None,'DEFAULT',-1,0) ),
                
                ( 'ttwo',True, (None,'DEFAULT',0,0) ),
                ( 'ttwo',True, (None,'DEFAULT',0,3.14) ),
                ( 'ttwo',False, (None,'DEFAULT',-1,0) ),
                
                ( 'tthree',True, (None,'DEFAULT',0,0) ),
                ( 'tthree',True, (None,'DEFAULT',0,3.14) ),

                ( 'all',True, (None,'text1','text2',1,2,3,3.14)),
                ( 'all',False, (None,'text1','text2',-1,-1,3,3.14)),
                ( 'all',False, (None,'text1','text2',-1,2,3,3.14)),
                ( 'all',False, (None,'text1','text2',1,-1,3,3.14)),
              ]
     
        for i, test in enumerate(tests): 
            table_name, truth, row = test
            table =  self.bundle.schema.table(table_name)
            vd = table._get_validator()
            
            if truth:
                self.assertTrue(vd(row), "Test {} not 'true' for table '{}': {}".format(i+1,table_name,row))
                
            else:
                self.assertFalse(vd(row), "Test {} not 'false' for table '{}': {}".format(i+1,table_name,row))

        # Testing the "OR" join of multiple columns. 

        tests =[
                ( 'tone',True, (None,'VALUE',0,0) ), #1
                ( 'tone',True, (None,'VALUE',-1,0) ),
                ( 'tone',False, (None,'DEFAULT',0,0) ),
                ( 'tone',False, (None,'DEFAULT',-1,0) ),
                
                ( 'ttwo',True, (None,'DEFAULT',0,0) ), #5
                ( 'ttwo',True, (None,'DEFAULT',0,3.14) ),
                ( 'ttwo',False, (None,'DEFAULT',-1,0) ),
                
                ( 'tthree',True, (None,'DEFAULT',0,0) ), #8
                ( 'tthree',True, (None,'DEFAULT',0,3.14) ),
                
                ( 'all',True, (None,'text1','text2',1,2,3,3.14)), #10
                ( 'all',False, (None,'text1','text2',-1,-1,3,3.14)), #11
                ( 'all',True, (None,'text1','text2',-1,2,3,3.14)), #12
                ( 'all',True, (None,'text1','text2',1,-1,3,3.14)), #13
              ]
     
        for i, test in enumerate(tests): 
            table_name, truth, row = test
            table =  self.bundle.schema.table(table_name)
            vd =table._get_validator(and_join=False)
            if truth:
                self.assertTrue(vd(row), "Test {} not 'true' for table '{}': {}".format(i+1, table_name,row))
            else:
                self.assertFalse(vd(row), "Test {} not 'false' for table '{}': {}".format(i+1, table_name,row))

        
        # Test the hash functions. This test depends on the d_test values in geoschema.csv
        tests =[
        ( 'tone','A|1|', (None,'A',1,2) ), 
        ( 'ttwo','1|2|', (None,'B',1,2) ), 
        ( 'tthree','C|2|', (None,'C',1,2) )]
        
        import hashlib
        
        for i, test in enumerate(tests): 
            table_name, hashed_str, row = test
            table =  self.bundle.schema.table(table_name)

            m = hashlib.md5()
            m.update(hashed_str)
            
            self.assertEquals(int(m.hexdigest()[:14], 16), table.row_hash(row))
        
    def test_partition(self):
        from ambry.dbexceptions import ConflictError
        from ambry.identity import PartitionIdentity, PartitionNameQuery
        from ambry.partition.csv import CsvPartition
        from ambry.partition.hdf import HdfPartition

        self.bundle.clean()
        self.bundle.prepare()

        p = self.bundle.partitions.new_db_partition(time=10, space=10, data={'pid':'pid1'})

        p = self.bundle.partitions.new_csv_partition(time=20, space=20, data={'pid':'pid2'})
        self.assertIsInstance(p, CsvPartition )
        p = self.bundle.partitions.find_or_new_csv(time=20, space=20)
        self.assertIsInstance(p, CsvPartition)

        p = self.bundle.partitions.new_hdf_partition(space=30, data={'pid':'pid3'})
        self.assertIsInstance(p, HdfPartition)
        p = self.bundle.partitions.find_or_new_hdf(space=30)
        self.assertIsInstance(p, HdfPartition)

        with self.assertRaises(ConflictError):
            self.bundle.partitions.new_db_partition(time=10, space=10, data={'pid':'pid1'})

        with self.assertRaises(ConflictError):
            self.bundle.partitions.new_csv_partition(time=20, space=20, data={'pid':'pid21'})

        with self.assertRaises(ConflictError):
            self.bundle.partitions.new_hdf_partition(space=30, data={'pid':'pid31'})


        self.assertEqual(3, len(self.bundle.partitions.all))

        p = self.bundle.partitions.find_or_new(time=10, space=10)
        p.database.create() # Find will go to the library if the database doesn't exist.
        self.assertEqual(3, len(self.bundle.partitions.all))
        self.assertEquals('pid1',p.data['pid'] )
      
        p = self.bundle.partitions.find_or_new_csv(time=20, space=20)
        p.database.create()  
        self.assertEquals('pid2',p.data['pid'] ) 

        p = self.bundle.partitions.find_or_new_hdf(space=30)
        self.assertEquals('pid3',p.data['pid'] ) 

        p = self.bundle.partitions.find(PartitionNameQuery(time=10, space=10))
        self.assertEquals('pid1',p.data['pid'] )

        p = self.bundle.partitions.find(time=10, space=10)
        self.assertEquals('pid1', p.data['pid'])

        p = self.bundle.partitions.find(PartitionNameQuery(time=20, space=20))
        self.assertEquals('pid2',p.data['pid'] )

        p = self.bundle.partitions.find(time=20, space=20)
        self.assertEquals('pid2',p.data['pid'] )

        pnq3 = PartitionNameQuery(space=30)

        p = self.bundle.partitions.find(pnq3)
        self.assertEquals('pid3',p.data['pid'] ) 
         
        with self.bundle.session as s:
            p = self.bundle.partitions._find_orm(pnq3).first()
            p.data['foo'] = 'bar'
            s.add(p)


        bundle = Bundle()
        p = bundle.partitions.find(pnq3)
        print p.data 
        self.assertEquals('bar',p.data['foo'] ) 

        p = self.bundle.partitions.find(PartitionNameQuery(name='source-dataset-subset-variation-30-hdf'))
        self.assertTrue(p is not None)
        self.assertEquals('source-dataset-subset-variation-30-hdf', p.identity.sname)
 
        #
        # Create all possible combinations of partition names
        # 

        table = self.bundle.schema.tables[0]
        
        p = (('time','time2'),('space','space3'),('table',table.name),('grain','grain4'))
        p += p
        pids = {}
        for i in range(4):
            for j in range(4):
                pid = self.bundle.identity.as_partition(**dict(p[i:i+j+1]))
                pids[pid.fqname] = pid

        
        with self.bundle.session as s:
        
            # These two deletely bits clear out all of the old
            # partitions, to avoid a conflict with the next section. We also have
            # to delete the files, since create() adds a partition record to the database, 
            # and if one already exists, it will throw an Integrity Error.
            for p in self.bundle.partitions:
                if os.path.exists(p.database.path):
                    os.remove(p.database.path)
            
            for p in self.bundle.dataset.partitions:
                s.delete(p)

        import pprint

        pprint.pprint(sorted([ pid.fqname for pid in pids.values()]))

        bundle = Bundle()
        bundle.clean()
        bundle.prepare()

        for pid in pids.values():
            part = bundle.partitions.new_db_partition(**pid.dict)
            part.create()

            parts = bundle.partitions._find_orm(PartitionNameQuery(vid=pid.vid)).all()
            self.assertIn(pid.sname, [p.name for p in parts])

        
    def test_runconfig(self):
        """Check the the RunConfig expands  the library configuration"""
        from ambry.run import  get_runconfig, RunConfig
        
        rc = get_runconfig((os.path.join(self.bundle_dir,'test-run-config.yaml'),RunConfig.USER_CONFIG, RunConfig.USER_ACCOUNTS))

        l = rc.library('library1')
         
        self.assertEquals('database1', l['database']['_name'])
        self.assertEquals('filesystem1', l['filesystem']['_name'])
        self.assertEquals('filesystem2', l['filesystem']['upstream']['_name'])
        self.assertEquals('filesystem3', l['filesystem']['upstream']['upstream']['_name'])
        self.assertEquals('devtest.sandiegodata.org', l['filesystem']['upstream']['upstream']['account']['_name'])

    def test_build_bundle_hdf(self):

        bundle = Bundle()
        bundle.clean()
        bundle = Bundle()
        bundle.exit_on_fatal = False
        bundle.pre_prepare()
        bundle.prepare()
        bundle.post_prepare()
        bundle.pre_build()
        bundle.build_hdf()
        bundle.post_build()

    def test_build_bundle(self):  
        import shutil
              
        bundle = Bundle()
        
        shutil.copyfile(
                bundle.filesystem.path('meta','schema-edit-me.csv'),
                bundle.filesystem.path('meta','schema.csv'))
        
        try:
            bundle.clean()
            bundle = Bundle()   
            bundle.exit_on_fatal = False
            bundle.pre_prepare()
            bundle.prepare()
            bundle.post_prepare()
            bundle.pre_build()
            bundle.build_db_inserter_codes()
            bundle.post_build()
    
            # The second run will use the changes to the schema made in the
            # first run, due to the types errors in the  'coding' table. 
    
            bundle.clean()
            bundle = Bundle()   
            bundle.exit_on_fatal = False
            bundle.pre_prepare()
            bundle.prepare()
            bundle.post_prepare()
            bundle.pre_build()
            bundle.build_db_inserter_codes()
            bundle.post_build()


        finally:
            
            # Need to clean up to ensure that we're back to a good state.
            # This runs the normal build, which will be used by the other
            # tests. 

            shutil.copyfile(
                    bundle.filesystem.path('meta','schema-edit-me.csv'),
                    bundle.filesystem.path('meta','schema.csv'))      


            bundle.clean()
            bundle = Bundle()   
            bundle.exit_on_fatal = False
            bundle.pre_prepare()
            bundle.prepare()
            bundle.post_prepare()
            bundle.pre_build()
            bundle.build()
            bundle.post_build()


    def test_simple_build(self):
        import shutil

        bundle = Bundle()

        shutil.copyfile(
            bundle.filesystem.path('meta', 'schema-edit-me.csv'),
            bundle.filesystem.path('meta', 'schema.csv'))


        bundle.clean()
        bundle = Bundle()
        bundle.exit_on_fatal = False
        bundle.pre_prepare()
        bundle.prepare()

        print bundle.database.dsn

        bundle.post_prepare()
        bundle.pre_build()
        bundle.build()
        bundle.post_build()


    def test_bundle(self):
        from ambry.bundle import DbBundle
        b = self.bundle

        print b.info

        lb = DbBundle(b.database.path)
        print '-----'
        print lb._repr_html_()



def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite
      
if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())