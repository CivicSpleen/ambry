from ambry.bundle import Bundle

class Bundle(Bundle):
    """ """

    def meta_pipeline(self, source):
        """Construct the ETL pipeline for the meta phase"""
        from ambry.bundle.etl.pipeline import Pipeline, MergeHeader, MangleHeader, MapHeader

        source = self.source(source) if isinstance(source, basestring) else source

        return Pipeline(
            source=source.fetch().source_pipe(),
            coalesce_rows=MergeHeader(),
            mangle_header=MangleHeader()
        )

    def meta(self):

        pl = self.meta_pipeline('rent07')

        for i, row in enumerate(pl()):
            print row

            if i > 5:
                break