.. custom_loader:


Modifying the Loader
====================


Mangling Headers and Columns
****************************

To alter the text of a column, or all of the columns in a header, override the :py:meth:`~ambry.bundle.loader.LoaderBundle.mangle_header` method. 

.. autoclass:: ambry.bundle.loader.LoaderBundle   
   :members: mangle_column_name, mangle_header

Here is an example of mangling a whole header, from the ``census.gov-saipe-county`` bundle:

.. code-block:: python 
    
    def mangle_header(self, header):
        """Transform the header as it comes from the raw row generator into a column name. 
        
        All of the columns for the upper and lower limits of the confidence interval have the same names, 
        so this function appends to the confidence interface columns the name of the closest leftward
        column that is not for a confidence interval. So:
        
            all_ages, 90%_ci_lower_bound, 90%_ci_upper_bound,
            
        becomes:
        
            all_ages, 90%_ci_lower_bound_all_ages, 90%_ci_upper_bound_all_ages,
            
        
        """  
        lh = None
        new_header = []
        for i,n in enumerate(header):
            
            if '90' not in n:
                ln = n
            else:
                n = n+'_'+ln
            
            new_header.append(self.mangle_column_name(i, n))
    
        return new_header

Altering values in a row
************************

There are two ways to alter the value of a row, just before insertion. The :py:meth:`~ambry.bundle.loader.LoaderBundle.build_modify_row` method operates on a whole role, while casters can target specific columns. 

.. autoclass:: ambry.bundle.loader.LoaderBundle
   :members: build_modify_row

Here is a fairly common case modifying a row, adding a geographic identifier for a county:

.. code-block:: python  

    @property
    @memoize # Memoizing caches the output of the function afer the first call. 
    def county_map(self):
        """A Map of county names used in the dataset to GVIDs for counties."""
        return { (int(r['state']), int(r['county'])) : r['gvid'] 
                 for r in  self.library.dep('counties').partition.rows }
            
    def build_modify_row(self, row_gen, p, source, row):
        """Set the time and gvid columns"""
        # If the table has an empty year, and the soruce has a time that converts to an int,
        # set the time as a year.
        if not row.get('year', False) and source.time:
            try:
                row['year'] = int(source.time)
            except ValueError:
                pass
             
        if 'postal_code' in row:
            pass 
            
        row['county_gvid'] =  self.county_map.get((int(row['state_fips']), int(row['county_fips'])), None)
            
            
Also mote that this method sets the ``year`` column to the value from the source entry. This is particularly useful when multiple  sources are all being stored in the same partition. 


Typecasters operate on single values, and must be specified in the schema. First, either use one of the existin rtype casters, or create a new one in the bundle. They can have any name you'd like. 

.. autoclass:: ambry.bundle.loader.LoaderBundle
   :members: int_caster, real_caster
   
After you've selected, or created a caster, set its value in the schema, in a new column called ``d_caster``. 



