"""
"""

from ambry.bundle import Bundle
from ambry.bundle.etl.pipeline import Pipe, augment_pipeline


class Bundle(Bundle):
    """ """
    def build_pipeline(self, source):
        """Construct the ETL pipeline for the build phase"""

        from ambry.bundle.etl.pipeline import Pipeline, MergeHeader, MangleHeader, MapHeader, augment_pipeline, PrintRows

        pl =  Pipeline(
            source=source.fetch().source_pipe(),
            coalesce_rows=MergeHeader(),
            mangle_header=MangleHeader()
        )

        augment_pipeline(pl, PrintRows)

        return pl

    def build(self):

        for i, source in enumerate(self.sources):

            print
            print '===================================='
            print source.dest_table_name

            #p = self.partitions.new_partition(table='example', space=space, time=2010 + j)
            #p.clean()
            #p.run()