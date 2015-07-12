from ambry.bundle import Bundle

from ambry.bundle.etl.pipeline import Sink
from ambry.bundle.etl.intuit import TypeIntuiter

class MakeSourceTable(Sink):
    def run(self, *args, **kwargs):
        super(MakeSourceTable, self).run(500)

        ti = self.pipeline[TypeIntuiter]

        if not self.source.st_id:

            # Create the source table for matting.
            for c in ti.columns:
                self.source.source_table.add_column(c.position, source_header = c.header, dest_header = c.header,
                                                    datatype=c.resolved_type)

        self.source.dataset.commit()

class Bundle(Bundle):
    """ """

    def meta_pipeline(self, source):
        """Construct the ETL pipeline for the meta phase"""
        from ambry.bundle.etl.pipeline import Pipeline, MergeHeader, MangleHeader, MapHeader

        source = self.source(source) if isinstance(source, basestring) else source

        return Pipeline(
            source=source.fetch().source_pipe(),
            coalesce_rows=MergeHeader(),
            mangle_header=MangleHeader(),
            type_intuit=TypeIntuiter(),
            sink=MakeSourceTable()
        )

    def meta(self):

        for i, source in enumerate(self.sources):

            #if source.name != 'rent07':
            #    continue

            pl = self.do_meta_pipeline(source)
            pl.run()
