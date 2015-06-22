.. _narrated_bundles:

Example Bundles Explained
=========================

This guide presents the core parts of several example bundles. You can find these bundles by cloning the `example.com repository from Github <https://github.com/CivicKnowledge/example-bundles>`_

example.com-simple-loader
*************************

.. seealso:: `Github repo for this bundle  <https://github.com/CivicKnowledge/example-bundles/tree/master/example.com/simple-loader>`_

Loader bundles use the :py:class:`ambry.bundle.loader.CsvBundle` loader class to automatically perform most of the required operations on a set of CSV files. 

bundle.yaml
-----------

Except for the required values of ``title``, ``summary`` and a few others, the :file:`bundle.yaml` file is unchanged from the default. 

bundle.py
---------

Because this bundle is using a loader class on a well-formed CSV file, there is nothing in the bundle class file. 

.. code-block:: python 

    from ambry.bundle.loader import CsvBundle

    class Bundle(CsvBundle):
        pass

build.yaml
-----------

The ``sources`` sections is also very simple, since it only needs to specify the URL of the file to load. The key name of the source entry, ``example`` is also the table name, and matches the table in the :file:`meta/schema.csv` file. 

.. code-block:: yaml 

    sources:
        example:
            description: Random CSV data
            url: http://public.source.civicknowledge.com.s3.amazonaws.com/example.com/simple-example.csv
            
schema.csv
----------

After setting the source entry URL, this schema was created by running :command:`bambry meta`

.. csv-table:: Schema

    table,seq,column,is_pk,is_fk,id,type,size,description
    example,1,id,1,,t8TXOw9pl701,INTEGER,5,Random CSV data
    example,3,uuid,,,c8TXOw9pl701003,VARCHAR,36,uuid
    example,4,int,,,c8TXOw9pl701004,INTEGER,3,int
    example,5,float,,,c8TXOw9pl701005,REAL,,float
    

medicare.gov-compare-home_health
********************************

.. seealso:: `Github repo for this bundle  <https://github.com/CivicKnowledge/example-bundles/tree/master/medicare.gov/compare-home_health>`_

While the Home Health COmpare bundle also uses the :py:class:`ambry.bundle.loader.CsvBundle` loader class, it is more complex because there are multiple files distributed in one zip file, some of the files have a non-ascii encoding,  and there are integer fields that have strings. This bundle also includes a file that has a NULL ('\\0' ) which, without processing, would cause the python CSV reader to fail. 

bundle.yaml
-----------

Except for the required values of ``title``, ``summary`` and a few others, the :file:`bundle.yaml` file is unchanged from the default. 

bundle.py
---------

This bundle requires two custom methods to handle malformed input files. The :meth:`int_na_caster` handles int columns that have the value 'NA' in them. The method will return a NULL for values that can't be converted to integers. Normally, this sort of casting error will cause the build to fail. 

The :meth:`line_mangler` processes data just before it is fed into the CSV reader. One of the files has an embedded NULL ('\\0' )  that cases the CSV reader to fail. 

.. code-block:: python 
    :emphasize-lines: 6,12
    
    from ambry.bundle.loader import CsvBundle

    class Bundle(CsvBundle):

        @staticmethod
        def int_na_caster(v):
            try:
                return int(v)
            except ValueError:
                return None

        def line_mangler(self, source, l):

            return l.replace('\0', '')

build.yaml
-----------

The ``sources`` sections for this bundle has a few special features. It does not require a ``row_spec`` section, because the files have the header on line 1. However, because the URL is for a ZIP file, each source must refer to a file in the ZIP file. The ``file`` entries are regular expressions that match and return only one file from the ZIP file. 

The file for the ``hhcahps_prvdr`` entry has a ``latin-1`` encoding, which causes the unicodecsv CSV reader to choke. Specifying the correct wencoding fixes the problem. 

.. code-block:: yaml 
    :emphasize-lines: 5,11

    sources:
        casper_aspen_contacts:
            description: The state agency contact information for maintaining the home
                health agency information that resides on the CMS certification system.
            file: Home_Health_Compare_CASPER_ASPEN_Contacts
            url: http://data.medicare.gov/views/bg9k-emty/files/K2mijv-Kwa3BxIvmpxh3ZYiFHcn_15Cd4WbvhBb9m3s?filename=HHCompare_Revised_FlatFiles.zip
        hhcahps_prvdr:
            description: Information on the Patient Experience of Care Survey results
                for each home health agency.
            encoding: latin-1
            file: HHC_SOCRATA_HHCAHPS_PRVDR.csv
            url: http://data.medicare.gov/views/bg9k-emty/files/K2mijv-Kwa3BxIvmpxh3ZYiFHcn_15Cd4WbvhBb9m3s?filename=HHCompare_Revised_FlatFiles.zip
    
            
schema.csv
----------

The schema file for this bundle is too long and wide to include, but you can see it `in the github repo <https://github.com/CivicKnowledge/example-bundles/blob/master/medicare.gov/compare-home_health/meta/schema.csv>`_. Note the ``d_caster`` column, which includes the references to :meth:`int_na_caster` for the columns that require it. 
    
After tweaking the ``sources`` to properly extract the files fro the ZIP archive, this schema was generated with :command:`bambry meta`, after which the ``d_caster`` values were added manually. 

