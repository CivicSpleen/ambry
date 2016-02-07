#
# NOTE: This is actually issue 29 from ambry_sources. It's easier to test here.
#

from test.test_base import TestBase


class BundleWarehouse(TestBase):

    def test_column_named_index(self):

        l = self.library()

        b = l.bundle('build.example.com-casters')

        wh = b.warehouse('test')

        wh.clean()

        # This fails with: 'SQLError: SQLError: near "index": syntax error'
        wh.materialize('build.example.com-casters-simple')

