"""
"""

from ambry.bundle import Bundle
from ambry.bundle.etl.pipeline import Pipe, augment_pipeline


class Bundle(Bundle):
    """ """


    def build(self, table_name=None):

        for i, source in enumerate(self.sources):

            if table_name and source.dest_table_name != table_name:
                self.log('Skipping table {}'.format(source.dest_table_name))
                continue

            print
            print '===================================='
            print source.dest_table_name

            p = self.partitions.new_partition(table='example', space=space, time=2010 + j)
            p.clean()
            p.run()