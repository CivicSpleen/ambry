# -*- coding: utf-8 -*-

from test.proto import TestBase


class Test(TestBase):

    def test_index_column_fails(self):
        if self._db_type != 'sqlite':
            self.skipTest('SQLite tests are disabled.')

        bundle = self.import_single_bundle('build.example.com/generators')
        bundle.ingest()
        bundle.source_schema()
        bundle.schema()
        bundle.build()

        wh = bundle.warehouse('test')
        wh.clean()
        self.assertIsNotNone(wh.materialize('build.example.com-generators-demo'))
