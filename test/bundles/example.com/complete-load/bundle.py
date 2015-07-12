"""
"""

from ambry.bundle import Bundle
from ambry.bundle.etl.pipeline import Pipe, augment_pipeline


class Bundle(Bundle):
    """ """
    def build_pipeline(self, source):
        """Construct the ETL pipeline for the build phase"""

        from ambry.bundle.etl.pipeline import Pipeline, MergeHeader, MangleHeader, MapHeader, augment_pipeline

        return Pipeline(
            source=source.fetch().source_pipe(),
            coalesce_rows=MergeHeader(),
            mangle_header=MangleHeader()
        )

    def build(self):

        for i, source in enumerate(self.sources):

            print i, source.name

            pl = self.do_build_pipeline(source)

            for i, row in enumerate(pl.run()):
                print source.name, i, row

                if i > 5:
                    break