"""
Created on Jul 6, 2013

@author: eric
"""

from semantic_version import Version, Spec

from ambry.identity import DatasetNumber, TableNumber, ObjectNumber, ColumnNumber, PartitionNumber,\
    Name, NameQuery, PartitionName, PartitionNameQuery,\
    Identity, PartitionIdentity, NumberServer, GeneralNumber1

from .test_base import TestBase


class Test(TestBase):

    def tearDown(self):
        pass

    def test_id(self):
        dnn = 1000000
        rev = 100

        dn = DatasetNumber(dnn)
        self.assertEqual('d000004c92', str(dn))

        dn = DatasetNumber(dnn, rev)
        self.assertEqual('d000004c9201C', str(dn))

        self.assertEqual('d000004c9201C', str(ObjectNumber.parse(str(dn))))

        tn = TableNumber(dn, 1)

        self.assertEqual('t000004c920101C', str(tn))

        self.assertEqual('t000004c920101C', str(ObjectNumber.parse(str(tn))))

        tnnr = tn.rev(None)

        self.assertEqual('t000004c9201', str(tnnr))

        self.assertEqual('t000004c9201004', str(tnnr.rev(4)))

        # Other assignment classes

        # dnn = 62 * 62 + 11

        dn = DatasetNumber(62 ** 3 - 1, None, 'authoritative')
        self.assertEqual('dZZZ', str(dn))

        dn = DatasetNumber(62 ** 3 - 1, None, 'registered')
        self.assertEqual('d00ZZZ', str(dn))

        dn = DatasetNumber(62 ** 3 - 1, None, 'unregistered')
        self.assertEqual('d0000ZZZ', str(dn))

        dn = DatasetNumber(62 ** 3 - 1, None, 'self')
        self.assertEqual('d000000ZZZ', str(dn))

        tn = TableNumber(dn, 2)
        self.assertEqual('t000000ZZZ02', str(tn))

        cn = ColumnNumber(tn, 3)
        self.assertEqual('c000000ZZZ02003', str(cn))

        pn = dn.as_partition(5)
        self.assertEqual('p000000ZZZ005', str(pn))

    def test_name(self):

        name = Name(source='Source.com',
                    dataset='data set',
                    subset='sub set',
                    variation='vari ation',
                    type='Ty-pe',
                    part='Pa?rt',
                    version='0.0.1')

        self.assertEqual('source.com-data_set-sub_set-ty_pe-pa_rt-vari_ation-0.0.1', name.vname)

        name = Name(source='source.com',
                    dataset='dataset',
                    subset='subset',
                    variation='variation',
                    type='type',
                    part='part',
                    version='0.0.1')

        self.assertEqual('source.com-dataset-subset-type-part-variation', str(name))
        self.assertEqual('source.com-dataset-subset-type-part-variation-0.0.1', name.vname)

        name = name.clone()

        self.assertEqual('source.com-dataset-subset-type-part-variation', str(name))
        self.assertEqual('source.com-dataset-subset-type-part-variation-0.0.1', name.vname)

        part_name = PartitionName(time='time',
                                  space='space',
                                  table='table',
                                  grain='grain',
                                  format='format',
                                  segment=101,
                                  **name.dict
                                  )

        self.assertEqual('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-101',
                          str(part_name))
        self.assertEqual('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-101-0.0.1',
                          part_name.vname)

        part_name = part_name.clone()

        self.assertEqual('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-101',
                          str(part_name))
        self.assertEqual('source.com-dataset-subset-type-part-variation-table-time-space-grain-format-101-0.0.1',
                          part_name.vname)

        # Name Query

        name_query = NameQuery(source='source.com', dataset='dataset', vname='foobar', type=NameQuery.NONE)

        with self.assertRaises(NotImplementedError):
            name_query.path()

        d = name_query.dict

        self.assertEqual('<any>', d['subset'])
        self.assertEqual('<none>', d['type'])
        self.assertEqual('dataset', d['dataset'])

        name_query = name_query.clone()

        self.assertEqual('<any>', d['subset'])
        self.assertEqual('<none>', d['type'])
        self.assertEqual('dataset', d['dataset'])

        name_query_2 = name_query.with_none()

        self.assertEqual(None, name_query_2.dict['type'])

        # With a semantic version spec

        name = Name(source='source.com', dataset='dataset', variation='orig', version='0.0.1')
        self.assertEqual('source.com-dataset-orig-0.0.1', name.vname)

        name.version_major = 2
        name.version_build = ('foobar',)

        self.assertEqual('source.com-dataset-orig-2.0.1+foobar', name.vname)

        name = Name(source='source.com', dataset='dataset', variation='variation', version='>=0.0.1')

        self.assertEqual('source.com-dataset-variation->=0.0.1', name.vname)

        name = Name(source='source.com', dataset='dataset', variation='variation', version='0.0.1')

        self.assertEqual('source.com/dataset-variation-0.0.1', name.path)

        self.assertEqual('source.com/dataset-variation', name.source_path)

        self.assertEqual('source.com/dataset-variation-0.0.1', name.cache_key)

        part_name = PartitionName(time='time',
                                  space='space',
                                  table='table',
                                  grain='grain',
                                  format='format',
                                  segment='segment',
                                  **name.dict
                                  )

        self.assertEqual('source.com/dataset-variation-0.0.1/table/time-space/grain-segment', part_name.path)

        part_name = PartitionName(time='time',
                                  space='space',
                                  table='table',
                                  format='db',
                                  **name.dict
                                  )

        self.assertEqual('source.com-dataset-variation-table-time-space', part_name.name)
        self.assertEqual('source.com-dataset-variation-table-time-space-0.0.1', part_name.vname)
        self.assertEqual('source.com/dataset-variation-0.0.1/table/time-space', part_name.path)

        part_name = PartitionName(time='time',
                                  space='space',
                                  format='format',
                                  **name.dict
                                  )

        self.assertEqual('source.com/dataset-variation-0.0.1/time-space', part_name.path)

        # Cloning

        part_name = name.as_partition(time='time',
                                      space='space',
                                      table='table',
                                      format='geo')

        self.assertEqual('source.com-dataset-variation-table-time-space-geo-0.0.1', part_name.vname)

    def test_identity(self):

        name = Name(source='source.com', dataset='foobar', version='0.0.1', variation='orig')
        dn = DatasetNumber(10000, 1, assignment_class='registered')

        ident = Identity(name, dn)

        self.assertEqual('d002Bi', ident.id_)
        self.assertEqual('d002Bi001', ident.vid)
        self.assertEqual('source.com-foobar-orig', str(ident.name))
        self.assertEqual('source.com-foobar-orig-0.0.1', ident.vname)
        self.assertEqual('source.com-foobar-orig-0.0.1~d002Bi001', ident.fqname)
        self.assertEqual('source.com/foobar-orig-0.0.1', ident.path)
        self.assertEqual('source.com/foobar-orig', ident.source_path)
        self.assertEqual('source.com/foobar-orig-0.0.1', ident.cache_key)

        self.assertEqual('source.com-foobar-orig-0.0.1', ident.name.dict['vname'])

        self.assertEqual({'id', 'vid', 'revision', 'name', 'vname', 'cache_key',
                           'variation', 'dataset', 'source', 'version'},
                          set(ident.dict.keys()))

        self.assertIn('fqname', ident.names_dict)
        self.assertIn('vname', ident.names_dict)
        self.assertNotIn('dataset', ident.names_dict)

        self.assertIn('dataset', ident.ident_dict)
        self.assertNotIn('fqname', ident.ident_dict)

        # Clone to get a PartitionIdentity

        pi = ident.as_partition(7)
        self.assertEqual('source.com-foobar-orig-0.0.1~p002Bi007001', pi.fqname)

        pi = ident.as_partition(8, time='time',
                                space='space',
                                format='geo')

        self.assertEqual('source.com-foobar-orig-time-space-geo-0.0.1~p002Bi008001', pi.fqname)

        # PartitionIdentity

        part_name = PartitionName(time='time', space='space', format='geo', **name.dict)

        pn = PartitionNumber(dn, 500)

        ident = PartitionIdentity(part_name, pn)

        expected_keys = set([
            'id', 'vid', 'revision', 'cache_key', 'name', 'vname', 'space', 'format', 'variation',
            'dataset', 'source', 'version', 'time'])
        self.assertEqual(expected_keys, set(ident.dict.keys()))

        self.assertEqual('p002Bi084', ident.id_)
        self.assertEqual('p002Bi084001', ident.vid)
        self.assertEqual('source.com-foobar-orig-time-space-geo', str(ident.name))
        self.assertEqual('source.com-foobar-orig-time-space-geo-0.0.1', ident.vname)
        self.assertEqual('source.com-foobar-orig-time-space-geo-0.0.1~p002Bi084001', ident.fqname)
        self.assertEqual('source.com/foobar-orig-0.0.1/time-space', ident.path)
        self.assertEqual('source.com/foobar-orig-0.0.1/time-space', ident.cache_key)

        # Updating partition names that were partially specified

        PartitionNameQuery(time='time', space='space', format='hdf')
        # pnq = PartitionNameQuery(time='time', space='space', format='hdf')

        # Partitions, converting to datasets

        ident = Identity(name, dn)
        pi = ident.as_partition(8, time='time', space='space', format='geo')

        self.assertEqual('source.com-foobar-orig-time-space-geo-0.0.1~p002Bi008001', pi.fqname)

        iid = pi.as_dataset()

        self.assertEqual(ident.fqname, iid.fqname)


    def test_identity_from_dict(self):

        name = Name(source='source.com', dataset='foobar', variation='orig', version='0.0.1')
        dataset_number = DatasetNumber(10000, 1, assignment_class='registered')

        oident = Identity(name, dataset_number)
        opident = oident.as_partition(7)

        idict = oident.dict
        pidict = opident.dict

        ident = Identity.from_dict(idict)

        self.assertIsInstance(ident, Identity)
        self.assertEqual(ident.fqname, oident.fqname)

        ident = Identity.from_dict(pidict)

        self.assertEqual('source.com/foobar-orig-0.0.1', ident.cache_key)

    def test_split(self):

        name = Name(source='source.com', dataset='foobar', version='1.2.3')
        dn = DatasetNumber(10000, 1, assignment_class='registered')

        # NOTE, version is entered as 1.2.3, but will be changed to 1.2.1 b/c
        # last digit is overridden by revision

        ident = Identity(name, dn)

        ip = Identity.classify(name)
        self.assertEqual(Name, ip.isa)
        self.assertIsNone(ip.version)

        ip = Identity.classify(ident.name)

        self.assertEqual(Name, ip.isa)
        self.assertIsNone(ip.on)
        self.assertEqual(ident.sname, ip.name)
        self.assertIsNone(ip.version)

        ip = Identity.classify(ident.vname)
        self.assertEqual(Name, ip.isa)
        self.assertIsNone(ip.on)
        self.assertEqual(ident.vname, ip.name)
        self.assertEqual(ident._name.version, str(ip.version))

        ip = Identity.classify(ident.fqname)
        self.assertEqual(DatasetNumber, ip.isa)
        self.assertEqual(ident.vname, ip.name)
        self.assertEqual(str(ip.on), str(ip.on))

        ip = Identity.classify(ident.vid)
        self.assertEqual(DatasetNumber, ip.isa)

        ip = Identity.classify(ident.id_)
        self.assertEqual(DatasetNumber, ip.isa)

        ip = Identity.classify(dn)
        self.assertEqual(DatasetNumber, ip.isa)

        ip = Identity.classify(dn.as_partition(10))
        self.assertEqual(PartitionNumber, ip.isa)

        ip = Identity.classify('source.com-foobar-orig')
        self.assertIsNone(ip.version)
        self.assertEqual('source.com-foobar-orig', ip.sname)
        self.assertIsNone(ip.vname)

        ip = Identity.classify('source.com-foobar-orig-1.2.3')
        self.assertIsInstance(ip.version, Version)
        self.assertEqual('source.com-foobar-orig', ip.sname)
        self.assertEqual('source.com-foobar-orig-1.2.3', ip.vname)

        ip = Identity.classify('source.com-foobar-orig->=1.2.3')
        self.assertIsInstance(ip.version, Spec)
        self.assertEqual('source.com-foobar-orig', ip.sname)
        self.assertIsNone(ip.vname)

        ip = Identity.classify('source.com-foobar-orig-==1.2.3')
        self.assertIsInstance(ip.version, Spec)
        self.assertEqual('source.com-foobar-orig', ip.sname)
        self.assertIsNone(ip.vname)

    # TODO: This test should run against a number server setup locally, not the main server.
    def x_test_number_service(self):
        # For this test, setup these access keys in the
        # Redis Server:
        #
        # redis-cli set assignment_class:test-ac-authoritative authoritative
        # redis-cli set assignment_class:test-ac-registered registered
        # redis-cli set assignment_class:fe78d179-8e61-4cc5-ba7b-263d8d3602b9 unregistered

        from ambry.run import get_runconfig
        from ambry.dbexceptions import ConfigurationError

        rc = get_runconfig()

        try:
            # ng = rc.service('numbers')
            rc.service('numbers')
        except ConfigurationError:
            return

        # You'll need to run a local service at this address
        host = "numbers"
        port = 7977
        unregistered_key = 'fe78d179-8e61-4cc5-ba7b-263d8d3602b9'

        ns = NumberServer(host=host, port=port, key='test-ac-registered')

        n = next(ns)
        self.assertEqual(6, len(str(n)))

        # Next request is authoritative, so no need to sleep here.
        ns = NumberServer(host=host, port=port, key='test-ac-authoritative')

        n = next(ns)
        self.assertEqual(4, len(str(n)))

        ns.sleep()  # Avoid being rate limited

        # Override to use a local numbers server:
        ns = NumberServer(host=host, port=port, key=unregistered_key)
        n = next(ns)
        self.assertEqual(8, len(str(n)))

        n1 = ns.find('foobar')

        self.assertEqual(str(n1), str(ns.find('foobar')))
        self.assertEqual(str(n1), str(ns.find('foobar')))

    def test_time_space(self):

        name = Name(source='source.com',
                    dataset='foobar',
                    version='0.0.1',
                    btime='2010P5Y',
                    bspace='space',
                    variation='orig')

        self.assertEqual('source.com-foobar-space-2010p5y-orig-0.0.1', name.vname)
        self.assertEqual('source.com/foobar-space-2010p5y-orig-0.0.1', name.cache_key)
        self.assertEqual('source.com/foobar-space-2010p5y-orig-0.0.1', name.path)
        self.assertEqual('source.com/space/foobar-2010p5y-orig', name.source_path)

    def test_partial_partition_name(self):

        from ambry.identity import PartialPartitionName, Name
        from itertools import combinations

        name_parts = ['table',  'time', 'space', 'grain', 'format', 'segment']

        ds_name = Name(source='source', dataset='dataset')

        # TODO: What is testing here?
        for s in list(combinations(name_parts, 3)):
            p = PartialPartitionName(**dict(list(zip(s, s))))
            self.assertIn('name', p.promote(ds_name).dict)

            p2 = PartialPartitionName(**dict(list(zip(s, s))))

            self.assertEqual(p, p2)

            p3 = PartialPartitionName(**dict(list(zip(s[1:], s[1:]))))

            self.assertNotEqual(p, p3)

    def test_parse_other(self):

        dn = DatasetNumber(100, 5, 'authoritative')

        self.assertEqual('d01C005', str(dn))

        self.assertEqual('G01C001Z005', str(GeneralNumber1('G',dn, 123)))

        self.assertEqual('G01C001Z005', ObjectNumber.parse(str(GeneralNumber1('G',dn, 123)), 'other1'))
