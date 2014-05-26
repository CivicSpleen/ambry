"""
Created on Jul 6, 2013

@author: eric
"""
import unittest

from ambry.identity import *
from  bundles.testbundle.bundle import Bundle
from test_base import  TestBase


class Test(TestBase):

    def setUp(self):

        self.copy_or_build_bundle()

        self.bundle = Bundle()
        self.bundle_dir = self.bundle.bundle_dir

    def tearDown(self):
        pass

    def test_id(self):
        dnn = 1000000
        rev = 100
        
        dn = DatasetNumber(dnn)
        self.assertEquals('d000004c92', str(dn))
        
        dn = DatasetNumber(dnn, rev)
        self.assertEquals('d000004c9201C', str(dn))

        self.assertEquals('d000004c9201C', str(ObjectNumber.parse(str(dn))))

        tn = TableNumber(dn, 1)

        self.assertEquals('t000004c920101C', str(tn))

        self.assertEquals('t000004c920101C', str(ObjectNumber.parse(str(tn))))

        tnnr = tn.rev(None)
        
        self.assertEquals('t000004c9201', str(tnnr))

        self.assertEquals('t000004c9201004', str(tnnr.rev(4)))

        # Other assignment classes
        
        dnn = 62*62+11
        
        dn = DatasetNumber(62**3-1,None,'authoritative')
        self.assertEquals('dZZZ', str(dn))
        
        dn = DatasetNumber(62**3-1,None,'registered')
        self.assertEquals('d00ZZZ', str(dn))
        
        dn = DatasetNumber(62**3-1,None,'unregistered')
        self.assertEquals('d0000ZZZ', str(dn))

        dn = DatasetNumber(62**3-1,None,'self')
        self.assertEquals('d000000ZZZ', str(dn))
        
        tn = TableNumber(dn, 2)
        self.assertEquals('t000000ZZZ02', str(tn))

        cn = ColumnNumber(tn, 3)
        self.assertEquals('c000000ZZZ02003', str(cn))

        pn = dn.as_partition(5)
        self.assertEquals('p000000ZZZ005', str(pn))

    def test_name(self):

        name = Name(source='Source.com',
                    dataset='data set',
                    subset='sub set',
                    variation='vari ation',
                    type='Ty-pe',
                    part='Pa?rt',
                    version='0.0.1')

        self.assertEquals('source.com-data_set-sub_set-ty_pe-pa_rt-vari_ation-0.0.1', name.vname)

        name = Name(source='source.com', 
                    dataset='dataset', 
                    subset='subset', 
                    variation='variation', 
                    type='type', 
                    part='part', 
                    version='0.0.1')

        self.assertEquals('source.com-dataset-subset-type-part-variation', str(name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-0.0.1', name.vname)

        name = name.clone()
        
        self.assertEquals('source.com-dataset-subset-type-part-variation', str(name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-0.0.1', name.vname)        


        part_name = PartitionName(time = 'time',
                                  space='space',
                                  table='table',
                                  grain='grain',
                                  format='format',
                                  segment=101,
                                  **name.dict
                                  )

        self.assertEquals('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-101',
                          str(part_name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-101-0.0.1',
                          part_name.vname)

        part_name = part_name.clone()
        
        self.assertEquals('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-101',
                          str(part_name))
        self.assertEquals('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-101-0.0.1',
                          part_name.vname)        

        # Name Query

        name_query = NameQuery(source='source.com', dataset='dataset', vname='foobar', type=NameQuery.NONE)

        with self.assertRaises(NotImplementedError):
            name_query.path

        d = name_query.dict

        self.assertEquals('<any>',d['subset'])
        self.assertEquals('<none>',d['type'])
        self.assertEquals('dataset',d['dataset'])

        name_query = name_query.clone()

        self.assertEquals('<any>',d['subset'])
        self.assertEquals('<none>',d['type'])
        self.assertEquals('dataset',d['dataset'])

        name_query_2 = name_query.with_none()
        
        self.assertEquals(None,name_query_2.dict['type'])

        # With a semantic version spec
        
        name = Name(source='source.com', dataset = 'dataset', variation='orig', version='0.0.1')
        self.assertEquals('source.com-dataset-orig-0.0.1',name.vname)  
        
        name.version_major = 2
        name.version_build = ('foobar',)

        self.assertEquals('source.com-dataset-orig-2.0.1+foobar',name.vname)  
        
        name = Name(source='source.com', dataset='dataset',variation='variation', version='>=0.0.1')

        self.assertEquals('source.com-dataset-variation->=0.0.1',name.vname)   

        name = Name(source='source.com', dataset='dataset',variation='variation', version='0.0.1')

        self.assertEquals('source.com/dataset-variation-0.0.1',name.path)   

        self.assertEquals('source.com/dataset-variation',name.source_path) 

        self.assertEquals('source.com/dataset-variation-0.0.1.db',name.cache_key) 

        part_name = PartitionName(time = 'time',
                                  space='space',
                                  table='table',
                                  grain='grain',
                                  format='format',
                                  segment='segment',
                                  **name.dict
                                  )


        self.assertEquals('source.com/dataset-variation-0.0.1/table/time-space/grain-segment',part_name.path)
         
        part_name = PartitionName(time = 'time',
                                  space='space',
                                  table='table',
                                  format='db',
                                  **name.dict
                                  )


        self.assertEquals('source.com-dataset-variation-table-time-space',part_name.name)
        self.assertEquals('source.com-dataset-variation-table-time-space-0.0.1',part_name.vname)
        self.assertEquals('source.com/dataset-variation-0.0.1/table/time-space',part_name.path)

        part_name = PartitionName(time = 'time',
                                  space='space',
                                  format='format',
                                  **name.dict
                                  )
   
        self.assertEquals('source.com/dataset-variation-0.0.1/time-space',part_name.path)


        pname = PartialPartitionName(time = 'time',
                                  space='space',
                                  table='table',
                                  format='csv'
                                  )

        part_name = pname.promote(name)
        
        self.assertEquals('source.com-dataset-variation-table-time-space-csv-0.0.1',part_name.vname)

        # Cloning

        part_name = name.as_partition(time = 'time',
                                  space='space',
                                  table='table',
                                  format='geo')

        self.assertEquals('source.com-dataset-variation-table-time-space-geo-0.0.1',part_name.vname)

    def test_identity(self):

        name = Name(source='source.com', dataset='foobar',  version='0.0.1', variation='orig')
        dn = DatasetNumber(10000, 1, assignment_class='registered')
        
        ident = Identity(name, dn)

        self.assertEquals('d002Bi',ident.id_)   
        self.assertEquals('d002Bi001',ident.vid)   
        self.assertEquals('source.com-foobar-orig',str(ident.name))   
        self.assertEquals('source.com-foobar-orig-0.0.1',ident.vname)   
        self.assertEquals('source.com-foobar-orig-0.0.1~d002Bi001',ident.fqname)   
        self.assertEquals('source.com/foobar-orig-0.0.1',ident.path) 
        self.assertEquals('source.com/foobar-orig',ident.source_path) 
        self.assertEquals('source.com/foobar-orig-0.0.1.db',ident.cache_key)

        self.assertEquals('source.com-foobar-orig-0.0.1', ident.name.dict['vname'])
        
        self.assertEquals({'id','vid','revision','name', 'vname', 'cache_key',
                               'variation', 'dataset', 'source', 'version'},
                          set(ident.dict.keys()))

        self.assertIn('fqname', ident.names_dict)
        self.assertIn('vname', ident.names_dict)
        self.assertNotIn('dataset', ident.names_dict)

        self.assertIn('dataset', ident.ident_dict)
        self.assertNotIn('fqname', ident.ident_dict)

        # Clone to get a PartitionIdentity

        pi = ident.as_partition(7)
        self.assertEquals('source.com-foobar-orig-0.0.1~p002Bi007001',pi.fqname)

        pi = ident.as_partition(8,time = 'time',
                                  space='space',
                                  format='hdf')

        self.assertEquals('source.com-foobar-orig-time-space-hdf-0.0.1~p002Bi008001',pi.fqname)


        # PartitionIdentity

        part_name = PartitionName(time = 'time',
                                  space='space',
                                  format='hdf',
                                  **name.dict
                                  )
        
        pn = PartitionNumber(dn, 500)
        
        ident = PartitionIdentity.new_subclass(part_name, pn)


        self.assertEquals(set(['id','vid','revision', 'cache_key',
                               'name', 'vname', 'space', 'format',
                               'variation', 'dataset', 'source', 
                               'version', 'time']), 
                          set(ident.dict.keys()))
        
        self.assertEquals('p002Bi084',ident.id_)   
        self.assertEquals('p002Bi084001',ident.vid)   
        self.assertEquals('source.com-foobar-orig-time-space-hdf',str(ident.name))
        self.assertEquals('source.com-foobar-orig-time-space-hdf-0.0.1',ident.vname)
        self.assertEquals('source.com-foobar-orig-time-space-hdf-0.0.1~p002Bi084001',ident.fqname)
        self.assertEquals('source.com/foobar-orig-0.0.1/time-space',ident.path)
        self.assertEquals('source.com/foobar-orig-0.0.1/time-space.hdf',ident.cache_key)
        
        # Updating partition names that were partially specified
        
        pnq = PartitionNameQuery(time = 'time',
                          space='space',
                          format='hdf'
                          )
        #import pprint
        #pprint.pprint(pnq.dict)

        #
        # Locations
        #

        print str(ident.locations)
        self.assertEquals('      ', str(ident.locations))
        ident.locations.set(LocationRef.LOCATION.LIBRARY, 1)
        ident.locations.set(LocationRef.LOCATION.REMOTE, 2)
        ident.locations.set(LocationRef.LOCATION.SOURCE)
        self.assertEquals('LSR   ', str(ident.locations))

        # Partitions, converting to datasets

        ident = Identity(name, dn)
        pi = ident.as_partition(8, time='time',
                                space='space',
                                format='hdf')

        self.assertEquals('source.com-foobar-orig-time-space-hdf-0.0.1~p002Bi008001', pi.fqname)

        iid = pi.as_dataset()

        self.assertEquals(ident.fqname, iid.fqname)


    def test_identity_from_dict(self):
        from ambry.partition.sqlite import SqlitePartitionIdentity
        from old.partition.hdf import HdfPartitionIdentity
        from ambry.partition.csv import CsvPartitionIdentity
        from ambry.partition.geo import GeoPartitionIdentity

        name = Name(source='source.com', dataset='foobar',  variation='orig', version='0.0.1')
        dn = DatasetNumber(10000, 1, assignment_class='registered')

        oident = Identity(name, dn)
        opident = oident.as_partition(7)

        idict  = oident.dict
        pidict = opident.dict

        ident = Identity.from_dict(idict)

        self.assertIsInstance(ident, Identity)
        self.assertEquals(ident.fqname, oident.fqname)

        ident = Identity.from_dict(pidict)
        self.assertIsInstance(ident, SqlitePartitionIdentity)
        self.assertEquals('source.com/foobar-orig-0.0.1.db', ident.cache_key)

        pidict['format'] = 'hdf'
        ident = Identity.from_dict(pidict)
        self.assertIsInstance(ident, HdfPartitionIdentity)
        self.assertEquals('source.com/foobar-orig-0.0.1.hdf', ident.cache_key)

        pidict['format'] = 'csv'
        ident = Identity.from_dict(pidict)
        self.assertIsInstance(ident, CsvPartitionIdentity)
        self.assertEquals('source.com/foobar-orig-0.0.1.csv', ident.cache_key)

        pidict['format'] = 'geo'
        ident = Identity.from_dict(pidict)
        self.assertIsInstance(ident, GeoPartitionIdentity)
        self.assertEquals('source.com/foobar-orig-0.0.1.geodb', ident.cache_key)


    def test_split(self):
        from semantic_version import Spec
        name = Name(source='source.com', dataset='foobar',  version='1.2.3')
        dn = DatasetNumber(10000, 1, assignment_class='registered')

        # NOTE, version is entered as 1.2.3, but will be changed to 1.2.1 b/c
        # last digit is overridden by revision

        ident = Identity(name, dn)

        ip = Identity.classify(name)
        self.assertEquals(Name, ip.isa)
        self.assertIsNone(ip.version)

        ip = Identity.classify(ident.name)

        self.assertEquals(Name, ip.isa)
        self.assertIsNone(ip.on)
        self.assertEquals(ident.sname, ip.name)
        self.assertIsNone(ip.version)

        ip = Identity.classify(ident.vname)
        self.assertEquals(Name, ip.isa)
        self.assertIsNone(ip.on)
        self.assertEquals(ident.vname, ip.name)
        self.assertEquals(ident._name.version, str(ip.version))

        ip = Identity.classify(ident.fqname)
        self.assertEquals(DatasetNumber, ip.isa)
        self.assertEquals(ident.vname, ip.name)
        self.assertEquals(str(ip.on), str(ip.on))

        ip = Identity.classify(ident.vid)
        self.assertEquals(DatasetNumber, ip.isa)

        ip = Identity.classify(ident.id_)
        self.assertEquals(DatasetNumber, ip.isa)

        ip = Identity.classify(dn)
        self.assertEquals(DatasetNumber, ip.isa)

        ip = Identity.classify(dn.as_partition(10))
        self.assertEquals(PartitionNumber, ip.isa)

        ip = Identity.classify("source.com-foobar-orig")
        self.assertIsNone(ip.version)
        self.assertEquals('source.com-foobar-orig',ip.sname)
        self.assertIsNone(ip.vname)

        ip = Identity.classify("source.com-foobar-orig-1.2.3")
        self.assertIsInstance(ip.version, Version)
        self.assertEquals('source.com-foobar-orig',ip.sname)
        self.assertEquals('source.com-foobar-orig-1.2.3',ip.vname)

        ip = Identity.classify("source.com-foobar-orig->=1.2.3")
        self.assertIsInstance(ip.version, Spec)
        self.assertEquals('source.com-foobar-orig',ip.sname)
        self.assertIsNone(ip.vname)

        ip = Identity.classify("source.com-foobar-orig-==1.2.3")
        self.assertIsInstance(ip.version, Spec)
        self.assertEquals('source.com-foobar-orig',ip.sname)
        self.assertIsNone(ip.vname)


    def test_bundle_build(self):

        from ambry.dbexceptions import ConflictError

        bundle = Bundle()

        # Need to clear the library, or the Bundle's pre_prepare
        # will cancel the build if this version is already installed
        bundle.library.purge()

        bundle.exit_on_fatal = False
        bundle.clean()
        bundle.database.create()

        bp = bundle.partitions

        with bundle.session:
            bp._new_orm_partition(PartialPartitionName(time = 't1', space='s1'))
            bp._new_orm_partition(PartialPartitionName(time = 't1', space='s2'))
            bp._new_orm_partition(PartialPartitionName(time = 't1', space=None))
            bp._new_orm_partition(PartialPartitionName(time = 't2', space='s1'))
            bp._new_orm_partition(PartialPartitionName(time = 't2', space='s2'))
            bp._new_orm_partition(PartialPartitionName(time = 't2', space=None))


        with self.assertRaises(ConflictError):
            with bundle.session:
                bp._new_orm_partition(PartialPartitionName(time = 't1', space='s1'))

        pnq = PartitionNameQuery(time=NameQuery.ANY, space='s1')

        names = [p.vname
                 for p in bp._find_orm(pnq).all()]


        self.assertEqual({u'source-dataset-subset-variation-t2-s1-0.0.1',
                          u'source-dataset-subset-variation-t1-s1-0.0.1'},
                         set(names))

        names = [p.vname
                 for p in bp._find_orm(PartitionNameQuery(space=NameQuery.ANY)).all()]

        self.assertEqual(6,len(names))

        names = [p.vname
                 for p in bp._find_orm(PartitionNameQuery(time='t1',space=NameQuery.ANY)).all()]

        self.assertEqual({'source-dataset-subset-variation-t1-s2-0.0.1',
                              'source-dataset-subset-variation-t1-0.0.1',
                              'source-dataset-subset-variation-t1-s1-0.0.1'},
                         set(names))


        names = [p.vname
                 for p in bp._find_orm(PartitionNameQuery(time='t1',space=NameQuery.NONE)).all()]

        self.assertEqual({'source-dataset-subset-variation-t1-0.0.1'},
                         set(names))

        # Start over, use a higher level function to create the partitions

        bundle.close() # Or you'll get an OperationalError
        bundle = Bundle()
        bundle.exit_on_fatal = False
        bundle.clean()
        bundle.database.create()
        bp = bundle.partitions

        bp._new_partition(PartialPartitionName(time = 't1', space='s1'))
        self.assertEquals(1, len(bp.all))

        bp._new_partition(PartialPartitionName(time = 't1', space='s2'))
        self.assertEquals(2, len(bp.all))

        bp._new_partition(PartialPartitionName(time = 't1', space=None))
        bp._new_partition(PartialPartitionName(time = 't2', space='s1'))
        bp._new_partition(PartialPartitionName(time = 't2', space='s2'))
        bp._new_partition(PartialPartitionName(time = 't2', space=None))
        self.assertEquals(6, len(bp.all))

        names = [p.vname
                 for p in bp._find_orm(PartitionNameQuery(time='t1',space=NameQuery.ANY)).all()]

        self.assertEqual({'source-dataset-subset-variation-t1-s2-0.0.1',
                              'source-dataset-subset-variation-t1-0.0.1',
                              'source-dataset-subset-variation-t1-s1-0.0.1'},
                         set(names))


        # Start over, use a higher level function to create the partitions
        bundle.close()
        bundle = Bundle()
        bundle.exit_on_fatal = False
        bundle.clean()
        bundle.database.create()
        bp = bundle.partitions

        p = bp.new_db_partition(time = 't1', space='s1')
        self.assertEquals('source-dataset-subset-variation-t1-s1-0.0.1~piEGPXmDC8001001', p.identity.fqname)

        p = bp.find_or_new(time = 't1', space='s2')
        self.assertEquals('source-dataset-subset-variation-t1-s2-0.0.1~piEGPXmDC8002001', p.identity.fqname)

        # Duplicate
        p = bp.find_or_new(time = 't1', space='s2')
        self.assertEquals('source-dataset-subset-variation-t1-s2-0.0.1~piEGPXmDC8002001', p.identity.fqname)

        p = bp.find_or_new_hdf(time = 't2', space='s1')
        self.assertEquals('source-dataset-subset-variation-t2-s1-hdf-0.0.1~piEGPXmDC8003001', p.identity.fqname)

        p = bp.find_or_new_geo(time = 't2', space='s1')
        self.assertEquals('source-dataset-subset-variation-t2-s1-geo-0.0.1~piEGPXmDC8004001', p.identity.fqname)

        p = bp.find_or_new_csv(time = 't2', space='s1')
        self.assertEquals('source-dataset-subset-variation-t2-s1-csv-0.0.1~piEGPXmDC8005001', p.identity.fqname)


        # Ok! Build!

        bundle.close()
        bundle = Bundle()
        bundle.exit_on_fatal = False

        bundle.clean()
        bundle.pre_prepare()
        bundle.prepare()
        bundle.post_prepare()
        bundle.pre_build()
        bundle.build_db_inserter_codes()
        bundle.post_build()

        self.assertEquals('diEGPXmDC8001',bundle.identity.vid)
        self.assertEquals('source-dataset-subset-variation',bundle.identity.sname)
        self.assertEquals('source-dataset-subset-variation-0.0.1',bundle.identity.vname)
        self.assertEquals('source-dataset-subset-variation-0.0.1~diEGPXmDC8001',bundle.identity.fqname)

    def test_number_service(self):
        
        ## For this test, setup these access keys in the
        ## Redis Server:
        ##
        ## redis-cli set assignment_class:test-ac-authoritative authoritative
        ## redis-cli set assignment_class:test-ac-registered registered
        ## redis-cli set assignment_class:fe78d179-8e61-4cc5-ba7b-263d8d3602b9 unregistered
        
        from ambry.identity import NumberServer
        from ambry.run import  get_runconfig
        rc = get_runconfig()
    
        ng = rc.group('numbers')

        # You'll need to run a local service at this address
        host = "numbers"
        port = 7977
        unregistered_key = 'fe78d179-8e61-4cc5-ba7b-263d8d3602b9'

        ns = NumberServer(host=host, port=port, key='test-ac-registered')

        n = ns.next()
        self.assertEqual(6,len(str(n)))

        # Next request is authoritative, so no need to sleep here.


        ns = NumberServer(host=host, port=port, key='test-ac-authoritative')

        n = ns.next()
        self.assertEqual(4,len(str(n)))

        ns.sleep() # Avoid being rate limited

        # Override to use a local numbers server:


        ns = NumberServer(host=host, port=port, key= unregistered_key)
        n = ns.next()
        self.assertEqual(8,len(str(n)))

        n1 = ns.find('foobar')

        self.assertEquals(str(n1), str(ns.find('foobar')))
        self.assertEquals(str(n1), str(ns.find('foobar')))

    #
    # This test is turned off because it doesn't delete the bundle at the end,
    # so the next test fails.
    #
    def x_test_rewrite(self):
        from  testbundle.bundle import Bundle
        import json
        from ambry.run import get_runconfig

        # Prepare to rewrite the bundle.yaml file.
        bundle = Bundle()
        orig = os.path.join(bundle.bundle_dir,'bundle.yaml')
        save = os.path.join(bundle.bundle_dir,'bundle.yaml.save')

        try:
            os.rename(orig,save)

            print 'Write to ', orig
            with open(orig,'w') as f:
                f.write(json.dumps(
                    {
                        "identity":{
                            "dataset": "dataset1",
                            "id": "dfoo",
                            "revision": 100,
                            "source": "source1",
                            "subset": "subset1",
                            "variation": "variation1",
                            "version": "1.0.1",
                            "vid": "dfob001",
                        },
                        "about": {
                            "author": "bob@bob.com"
                        }
                    }
                ))

            get_runconfig.clear() # clear config cache.
            bundle = Bundle()
            bundle.clean()
            bundle.pre_prepare()
            bundle.prepare()
            bundle.post_prepare() # Does the rewrite, adding the 'names'

            # Need to clear and reload one more time for the 'names' to appear
            get_runconfig.clear() # clear config cache.
            bundle = Bundle()
            bundle.exit_on_fatal = False

            self.assertEquals('dataset1', bundle.config.identity.dataset)
            self.assertEquals('dfoo', bundle.config.identity.id)
            self.assertEquals(100, bundle.config.identity.revision)

            self.assertEquals("source1-dataset1-subset1-variation1-1.0.100~dfoo01C", bundle.config.names.fqname)

            self.assertEquals("bob@bob.com", bundle.config.about.author)

        finally:
            os.rename(save, orig)
            self.delete_bundle()


    def test_format(self):

        name = Name(source='source.com', dataset='foobar',  version='0.0.1')
        dn = DatasetNumber(10000, 1, assignment_class='registered')

        for format in ('geo','hdf','csv','db'):
            pi = Identity(name, dn).as_partition(space='space', format=format)
            print type(pi), pi.path


    def test_time_space(self):

        name = Name(source='source.com',
                    dataset='foobar',
                    version='0.0.1',
                    btime='2010P5Y',
                    bspace='space',
                    variation='orig')

        self.assertEquals('source.com-foobar-space-2010p5y-orig-0.0.1', name.vname)
        self.assertEquals('source.com/foobar-space-2010p5y-orig-0.0.1.db', name.cache_key)
        self.assertEquals('source.com/foobar-space-2010p5y-orig-0.0.1', name.path)
        self.assertEquals('source.com/space/foobar-2010p5y-orig', name.source_path)

        return

        dn = DatasetNumber(10000, 1, assignment_class='registered')

        ident = Identity(name, dn)

        self.assertEquals('d002Bi', ident.id_)
        self.assertEquals('d002Bi001', ident.vid)
        self.assertEquals('source.com-foobar-orig', str(ident.name))
        self.assertEquals('source.com-foobar-orig-0.0.1', ident.vname)
        self.assertEquals('source.com-foobar-orig-0.0.1~d002Bi001', ident.fqname)
        self.assertEquals('source.com/foobar-orig-0.0.1', ident.path)
        self.assertEquals('source.com/foobar-orig', ident.source_path)
        self.assertEquals('source.com/foobar-orig-0.0.1.db', ident.cache_key)

        d = {
                'id': 'd002Bi',
                'source': 'source',
                'creator': 'creator',
                'dataset': 'dataset',
                'subset': 'subset',
                'btime': 'time',
                'bspace': 'space',
                'variation': 'variation',
                'revision': 1,
                'version': '0.0.1'
            }


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()