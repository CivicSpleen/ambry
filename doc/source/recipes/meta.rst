.. _recipes_meta_toplevel:

=========
Meta data
=========

Creating A Simple Schema
------------------------

When a schema is simple and can be computed, it may be easiest to 
create it directly from the source data, by downloading it and using the
header to create columns. 

.. sourcecode:: python

    def meta(self):
        '''Create a schema from the header of a CSV document'''
        import unicodecsv as csv
        import yaml

        # Download and extract the header. 
        fn = self.filesystem.download('deaths')

        with open(fn) as f:
            reader = csv.reader(f)
            header = reader.next()

        # We will be creating the bundle database to create the schema
        # then discarding it before the  build phase. 
        self.database.create()

        with self.session: 
            t = self.schema.add_table('table_name')

            for name in header:
                self.schema.add_column(t, name, datatype = 'integer')
        
        # Write the schema back out
        self.schema.write_schema()
        
        return True
        
Note that this script will add or overwrite the existing schema, so you may have to remove existing columns from the default schema first. 