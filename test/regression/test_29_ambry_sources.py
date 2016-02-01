# -*- coding: utf-8 -*-
from __future__ import print_function

from test.test_base import TestBase

# This test produces this error:
#
# Traceback (most recent call last):
#   File "/Users/eric/proj/virt/ambry-develop/ambry/test/regression/test_29_ambry_sources.py", line 22, in test_bundle_warehouse
#     print(wh.materialize('build.example.com-casters-simple'))
#   File "/Users/eric/proj/virt/ambry-develop/ambry/ambry/library/warehouse.py", line 129, in materialize
#     return self._backend.install(connection, partition, materialize=True)
#   File "/Users/eric/proj/virt/ambry-develop/ambry/ambry/mprlib/backends/sqlite.py", line 39, in install
#     self._add_partition(connection, partition)
#   File "/Users/eric/proj/virt/ambry-develop/ambry/ambry/mprlib/backends/sqlite.py", line 271, in _add_partition
#     sqlite_med.add_partition(connection, partition.datafile, partition.vid)
#   File "/Users/eric/proj/virt/ambry-develop/ambry_sources/ambry_sources/med/sqlite.py", line 135, in add_partition
#     cursor.execute(query)
#   File "src/vtable.c", line 175, in VirtualTable.xCreate.sqlite3_declare_vtab
# SQLError: SQLError: near "index": syntax error

class BundleWarehouse(TestBase):


    def test_index_column_fails(self):

        l = self.library()

        b = l.bundle('build.example.com-casters')

        wh = b.warehouse('test')

        wh.clean()

        print(wh.dsn)

        print(wh.materialize('build.example.com-casters-simple'))


