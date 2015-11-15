# -*- coding: utf-8 -*-

try:
    # py2, mock is external lib.
    from mock import patch
except ImportError:
    # py3, mock is included
    from unittest.mock import patch

from test.factories import PartitionFactory

from ambry.library import new_library
from ambry.run import get_runconfig

from test.test_base import TestBase

from ambry_sources import MPRowsFile
from ambry_sources.sources import GeneratorSource, SourceSpec


class Test(TestBase):

    def test_select_query(self):
        from ambry.library.warehouse import Warehouse

        # FIXME: Find the way how to initialize bundle with partitions and drop partition creation.
        library = self.__class__.library()
        bundle = self.setup_bundle('simple', source_url='temp://', library=library)
        PartitionFactory._meta.sqlalchemy_session = bundle.dataset.session
        partition1 = PartitionFactory(dataset=bundle.dataset)
        bundle.wrap_partition(partition1)

        # FIXME: Improve library constructor and set warehouse there.
        library._warehouse = Warehouse(library)

        def gen():
            # generate header
            yield ['col1', 'col2']

            # generate first row
            yield [0, 0]

            # generate second row
            yield [1, 1]

        datafile = MPRowsFile(bundle.build_fs, partition1.cache_key)
        datafile.load_rows(GeneratorSource(SourceSpec('foobar'), gen()))
        partition1._datafile = datafile
        rows = library.warehouse.query('SELECT * FROM {};'.format(partition1.vid))
        self.assertEqual(rows, [(0, 0), (1, 1)])
