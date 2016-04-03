
Tips
====


In the pipeline reports, check that headers for the ambry.etl.pipeline.MapSourceHeaders pipe are what you expect to be
fed into the rest of the pipeline.


Limited Runs
------------

THe :option:`-L` option to the :command:`bambry` command sets the limited_run flag, which is used to signal that a subset of rows should be processed. This flag can be check in generators, but for normal, non-generator sources, it is best to add a ``Head`` pipe to the pipeline. To do that, add this code to the bundle class:

.. code-block:: python

    def edit_pipeline(self, pl):
        """The -L option limits each source build to the first 400 rows"""
        from ambry.etl.pipeline import Head
        if self.limited_run:
            pl.first = [Head(400)]

        return pl

Excel Dates
-----------

Excel date are stored as floats and they have two different Zero dates, which is really stupid.

Use excel_dt_1900 and excel_dt_1904