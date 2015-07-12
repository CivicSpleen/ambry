.. _configuring_sources:


Configuring Sources
===================

.. seealso:: 

    :ref:`Solving common challenges and errors<common_challenges>`


In the :ref:`last section <bundle_configuration>` we created a new bundle and  set the basic metadata. Now we can configure sources and use the Loader class to create tables and load them with data. 

Source File Entries
*******************

Once you've setup the basic metadata, and in particular, set the download and/or dataset values, you can set the source URLS. These are references to the source files that will be loaded into the bundle. 

The easiest way to get these links is to run :command:`bambry config scrape`. This will extract the links from the pages specified by ``external_documentation.download`` and ``external_documentation.dataset``, looking for PDF, CSV and XLS files. It will dump the links in the proper formats for the ``sources`` and ``external_documentation`` sections. XLS and CSV files will go in the sources section, while PDF files will go in the external_documentation section. 

You can often just copy these into the configuration. The sources go into the ``sources`` section in the :file:`meta/build.yaml` file. You can also copy in the exteral_documentation values, but it's usually better to only copy the most important ones, since users will usually prefer to use the links from the original page, rather than from the Ambry documentation. 

For the USDA agricultural productivity example bundle, :command:`bambry config scrape` finds about 25 data links, most of which are named 'table' with a number, and have no description. It would be best to change the section keys to be more informative, but we'll do that later. For now, here is what the  ``sources`` section in :file:`meta/build.yaml` looks like: 

.. code-block:: yaml

    sources:
      StatePriceIndicesAndQ:
        description: None
        url: http://www.ers.usda.gov/dataFiles/Agricultural_Productivity_in_the_US/StateLevel_Tables_Price_Indicies_and_Implicit_Quantities/StatePriceIndicesAndQ.xls
      table01:
        description: none
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/National_Tables/table01.xls
      table02:
        description: None
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/National_Tables/table02.xls
      table03:
        description: None
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/StateLevel_Tables_Relative_Level_Indices_and_Growth_19602004Outputs/table03.xls
      table04:
        description: None
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/StateLevel_Tables_Relative_Level_Indices_and_Growth_19602004Outputs/table04.xls

Note that for this example, the :command:`bambry config scrape` reported all of the URLs as relative, starting with `/dataset`. The URLs were manually edited to add the schema and host. 


Creating Metadata
*****************


For the USDA agricultural productivity example bundle, you can now use the bundle to create the tables, using the ``meta`` phase, with the command :command:`bambry meta --clean`

.. code-block:: bash

    $ bambry meta
    INFO ers.usda.gov-agg_product ---- Cleaning ---
    INFO ers.usda.gov-agg_product Removing /Users/eric/proj/virt/ambry-master/data/source/example-bundles/ers.usda.gov/agg_product/build/ers.usda.gov/agg_product-0.0.1
    INFO ers.usda.gov-agg_product Removing /Users/eric/proj/virt/ambry-master/data/source/example-bundles/ers.usda.gov/agg_product/build/ers.usda.gov/agg_product-0.0.1.db
    INFO ers.usda.gov-agg_product Removing /Users/eric/proj/virt/ambry-master/data/source/example-bundles/ers.usda.gov/agg_product/build/ers.usda.gov/agg_product-0.0.1.log
    INFO ers.usda.gov-agg_product ---- Meta ----
    INFO ers.usda.gov-agg_product Loading protos from civicknowledge.com-proto-proto_terms-0.0.9~p001001009
    INFO ers.usda.gov-agg_product Created table table21
    INFO ers.usda.gov-agg_product Created table table20
    INFO ers.usda.gov-agg_product Created table table22
    ...
    ...
    ...
    
When the ``meta`` phase is finished, you will have three new files in the bundles ``meta`` directory. 

- :file:`meta/sources.csv`
- :file:`meta/column_map.csv`
- :file:`meta/schema.csv`

The :file:`meta/sources.csv` file is a spreadsheet version of the source information that was configured in the ``sources`` section of the :file:`meta/build.yaml` file. The spreadsheet version is a bit easier to edit, but the :command:`bambry config scrape` command doesnt output in the spreadsheet format yet. 

In the :file:`meta/column_map.csv` file, you'll find all of the column names that the ``meta`` phase found in the source file. This file can be used to alter column names to combine multiple columns together when creating one table from multiple files. 

The :file:`meta/schema.csv` is the main schema, with records of all of the tables and columns. 

Iif you are following along with the example, open up a few of those files , and you will notice that they are filled with tables named `table` with a number, and the column names are similarly generic. We'll have to do more exploration, using the files that have been added to the :file:`build` directory, which hold samples of the data from the source files. First, let's present the core objects in the ``meta`` phase, then look at how to fix these problems. 

Loader Classes
**************



The Loaders are subclasses of :py:class:`ambry.bundle.BuildBundle` that are tailored for loading datasets from CSV, Excel and Shapefile files.  These classes provide many special features to reduce the effort required to create a good bundle. 


Process Overview
----------------


The loader classes introduce a few new objects and use additional configuration. The new objects are:

* The :py:class:`ambry.bundle.loader.LoaderBundle` base class
* The :py:class:`ambry.bundle.rowgen.RowGenerator`, for yielding rows from a source file. 
* The :py:class:`ambry.bundle.rowgen.RowSpecIntuiter`, for guessing the structure of a CSV or Excel file that may have header ocmments
* :py:class:`ambry.util.intuit.Intuiter`, a class for quessing the schema of a source file. 

When using a loader as a base class, more work is done during the ``meta`` to prepare the schema, most of which is based on the additional configuration in the source metadata entries. Here is an example source that shows some of these features. 

.. code-block:: yaml

     sources:
         pqi:
             description: Multi-year PQI file.
             grain: counties
             time: 2012
             space: California
             row_spec:
                 data_end_line: null
                 data_start_line: 7
                 header_comment_lines:
                 - 0
                 - 1
                 - 2
                 - 3
                 - 5
                 header_lines:
                 - 4
                 - 6
             segment: 1
             url: http://www.oshpd.ca.gov/HID/Products/PatDischargeData/AHRQ/PQI/PQI_Summary_V45a_2005-2013.xlsx
  
  
At the lowest layer of the build process, when using loaders, is the  :py:class:`ambry.bundle.rowgen.RowGenerator`. The RowGenerator has an internal raw row generator that read the file specified in the URL and generates each row as a list. The outer RowGenerator object then uses the ``row_spec`` to yield just the data rows, returning headers and comments through special acessors. 
             
The ``row_spec``  is created by the :py:class:`RowSpecIntuiter` to distinguish the header, data and comments in the source file. This source file has several lines of comments, and the header spans multiple lines. The :py:class:`RowSpecIntuiter` usually makes a good first guess, but in this case, notice that there is a comment line ( 5) between two header lines ( 4 and 6 ). 

The ``segment`` tells the Intuiters and Row generators that the second workbook in the excel file should be used for this source. For Excel spreadsheets with multiple workbooks, there will be one source entry per workbook. In this fiel, the zeroth workbook has comments and documentation. 

Since this ``source`` entry does not specify a ``table``, this source entry will result in the creation of a table names ``pqi``, the key of the source entry. If the source entry has a ``table`` entry, the value of the entry will be used for the table name. This allows multiple source entries to contribute data to the same table. 

When using a Loader, the dataset specified by the source entry will get loaded into a partition. That partition will be constructed on the table name, and on the ``time``, ``space`` and ```grain` values, if they are set. ``Time`` is usually a year, but can be any ISO8601 date or duration. ``Space`` is a name that can be found in the places full text index, which includes, at least, all of the county and state names in the US. ( The system will look up the string value in the index and take the first one. You can check what it will get with :command:`ambry search -i <name>` )

``Grain`` is also used to set ``proto_vid`` values in the table. In this case, if the table has columns for ``year`` and ``gvid``, these columns will get foreign keys to the county index, by having their ``proto_vid`` set to ``counties.year`` and ``counties.gvid`` respectively. 

Due to the breadth of the ``sources`` configuration, most Loader bundles only require two methods to be implemented in the bundle :py:meth:`mangle_column_name`, to alter file header names into schema column name, and :py:meth:`build_modify_row` to alter each row before insertin into the partition, but usually only for setting the ``gvid`` column to a geographic geoid based on other values in the row. In many cases, neither of these methods are required, and the Loader bundle has a nearly empty :file:`bundle.py` file.
 

Using Sources to Alter Tables and Columns
*****************************************

With a basic understanding of the row specs and intuiters, we can setup the sources configuration to get the right data into our bundle. For the USDA agricultural productivity example bundle, after running :command:`bambry meta` you'll have pre and post row generator sample data from all of the sources. The :file:`build` directory wil have, among a lot of other similar files: 

* :file:`build/table01-raw-rows.csv` The rows directly from the `table` source, going into the RowGenerator
* :file:`build/table01-specd-rows.csv` The rows output from the RowGenerator, after applying the row specification

In our example, the  :file:`build/table01-raw-rows.csv` file starts with 5 lines that look like comments, and lines 6 and 7 both look like comments. You can uses these observations to create your own row spec, but it is easier to try a special function to intuit the row spec. Try running :command:`bambry run meta_set_row_specs`

.. code-block:: bash

    $ bambry run meta_set_row_specs

When the command finishes, it will have updated both the `sources` section of :file:`meta/build.yaml` and the spreadsheet version in :file:`meta/sources.csv`


.. important::
    The `sources` section of :file:`meta/build.yaml` has the same information as :file:`meta/sources.csv`. Ambry will replace the older of the two with the data from the one that has change most recently. To clear out the sources, you'll need to remove all of the records from both. Its a poor design, and doesn't work right all of the time, so you may have to delete all of one, the other, or both to get changes to propagate. Or try re-running :command:`bambry meta --clean` or :command:`bambry prepare`
    
The row intuiter often gets the `data_end_line` wrong. Often it's just best to delete that value, but for the USDA example, the value is guessed correctly, and it is the seperation between two different tables in the same file, which we will deal with later. 

.. important::
    If your file is a normal CSV file, with the header on line 1, you don't need to run :command:`bambry run meta_set_row_specs`, and you **don't even need the row_spec section.** Just ignore it and the Row Generator will assume the header is on line 1. 
    
    Always check the results of :command:`bambry run meta_set_row_specs` with the file. The intuiter often guesses wrong. 

The Row Intuiter does a pretty good job, but doesn't always get everything right. In our example, the row intuiter guessed these values for table01:

.. code-block:: yaml

    table01:
        description: None
        row_spec:
            data_end_line: 71
            data_start_line: 7
            header_comment_lines:
            - 0
            - 1
            - 2
            - 3
            - 4
            - 5
            header_lines:
            - 6
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/National_Tables/table01.xls

The values are 0 based, so when comparing the results to :file:`build/table01-raw-rows.csv`, the 0 row is row 1 in the spreadsheet. In this case, the intuiter guessed wrong: both rows 5 and 6 should be header rows. Move the 5 into the `header_lines` section so it has both 5 and 6. Then, rerun :command:`bambry meta` with the `--clean` option:

.. code-block:: bash

    $ bambry meta --clean 
    
Now, look in :file:`build/table01-specd-rows.csv`, and you will see that the data looks much more sensible, with a single header line with reasonable column names. Then, look at the `table01` entries in :file:`meta/schema.csv`. It should have good column names, descriptions, and datatypes that are sensible for the columns. The other tables in the  :file:`meta/schema.csv` are also probably improved, but the row_specs should be reviewed and edited to ensure they match the structure of the files. 

.. tip::

    It is easier to edit a single row spec in the :file:`meta/build.yaml` file, but with more than 5 or 6 sources, using  :file:`meta/sources.csv` is easier. 
 
Complex Tables
**************

For the USDA agricultural productivity example bundle, many of the files are unusually complex; they have an additional table of data after the first. See, for example, :file:`build/table03-raw-rows.csv`. The main table ends at line 54, there there is a second table that starts at line 55. This is a very unusual case, it can be handled with an additional source entry and row spec. Here is a new record you can add to `sources` that will access the table03 file again, but take the second table, rather than the first: 

.. code-block:: yaml

    table03_growth:
        description: None
        row_spec:
            data_start_line: 57
            header_comment_lines:
            - 56
            header_lines:
            - 5
        url: http://www.ers.usda.gov/datafiles/Agricultural_Productivity_in_the_US/StateLevel_Tables_Relative_Level_Indices_and_Growth_19602004Outputs/table03.xls
    
.. tip::

 Adding all of these extra growth tables by hand would be tedious, as are many other manipulations on a large set of sources. Fortunately, you can write function in the BUndle class to manipulate the metadata and create these values programatically. 
    
After running :command:`bambry meta --clean` again, the :file:`build` directory will have a :file:`build/table03_growth-specd-rows.csv` file that confirms that the new source entry has extracted the second table. 

Selecting Segments
******************

There is another complexity in this dataset. Table 1, for Farm Output, has two worksheets. By default, a source loads the first worksheet, but we can select other worksheets with the `segements` value. Copy the record for table01 to a new table, give it a new name, and set a `segment` value of 1. ( Segments are 0 based ) 

Here is a new sources block, with a segment entry, to extract the second worksheet. 

.. code-block:: yaml

    table03_growth_prices:
        comment: null
        conversion: null
        dd_url: null
        description: Indices of farm output, input, and total factor productivity
            for the United States, 1948-2011. Includes price indices and implicit
            quantities of farm outputs and inputs (see second tab in workbook), Table1a.
        file: null
        filetype: null
        foreign_key: null
        is_loadable: null
        row_data: null
        row_spec:
            data_end_line: 71
            data_start_line: 7
            header_comment_lines:
            - 0
            - 1
            - 2
            - 3
            header_lines:
            - 5
            - 6
        segment: 1

Choosing Table Names
********************
 
When building Ambry bundles, getting all of the metadata right isn't just important, is almost the only important task. So, at this point you should change all of the source entry keys, ( or the `name` column in the spreadsheet version ) to have a simple name that is indicative of the data in the table, because the 'name' field will become the table name when the data is loaded. ( Unless you have set a separate `table` value. ) For the USDA example, the dataset page has all of the information required to set sensible table names. ( If you are following along with the demo, just copy :file:`meta/sources.csv` from the `agg_product-demo` bundle into yours )

.. tip::

    In the `sources` metadata version of the file, the key to each sources entry is mapped to the `name` column in the spreadsheet version. The metadata keys have to be unique, because they are keys in a dictionary, but the `name` column value doesn't have to be unique. Regardless, the two versions of the file have to match up. So, if you set the key or the `name` column, but no value for `table`, the data schema will have a table based on the name. If you set a 'table' value, mutiple sources can be loaded into a single table. 
    
In the next section we will :ref:`configure the schema <configure_schema>` and buld the bundle.
    
    
