.. _build_schema:

The Loaders are subclasses of :py:class:`ambry.bundle.BuildBundle` that are tailored for loading datasets from CSV, Excel and Shapefile files.  These classes provide many special features to reduce the cload required to create a good bundle. 

Loader Classes
**************

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
  
At the lowest layer of the build process, when using loaders, is the  :py:class:`ambry.bundle.rowgen.RowGenerator`. The RowGenerator has an internal raw row generator that read the file specified in the URL and generates each row as a list. THe outer RowGenerator object then uses the ``row_spec`` to yield just the data rows, returning headers and comments through special acessors. 
             
The ``row_spec``  is created by the :py:class:`RowSpecIntuiter` to distinguish the header, data and comments in the source file. This source file has several lines of comments, and the header spans multiple lines. The :py:class:`RowSpecIntuiter` usually makes a good first guess, but in this case, notice that there is a comment line ( 5) between two header lines ( 4 and 6 ). 

The ``segment`` tells the Intuiters and Row generators that the second workbook in the excel file should be used for this source. For Excel spreadsheets with multiple workbooks, there will be one source entry per workbook. In this fiel, the zeroth workbook has comments and documentation. 

Since this ``source`` entry does not specify a ``table``, this source entry will result in the creation of a table names ``pqi``, the key of the source entry. If the source entry has a ``table`` entry, the value of the entry will be used for the table name. This allows multiple source entries to contribute data to the same table. 

When using a Loader, the dataset specified by the source entry will get loaded into a partition. That partition will be constructed on the table name, and on the ``time``, ``space`` and ```grain` values, if they are set. ``Time`` is usually a year, but can be any ISO8601 date or duration. ``Space`` is a name that can be found in the places full text index, which includes, at least, all of the county and state names in the US. ( The system will look up the string value in the index and take the first one. You can check what it will get with :command:`ambry search -i <name>` )

``Grain`` is also used to set ``proto_vid`` values in the table. In this case, if the table has columns for ``year`` and ``gvid``, these columns will get foreign keys to the county index, by having their ``proto_vid`` set to ``counties.year`` and ``counties.gvid`` respectively. 

Due to the breadth of the ``sources`` configuration, most Loader bundles only require two methods to be implemented in the bundle :py:meth:`mangle_column_name`, to alter file header names into schema column name, and :py:meth:`build_modify_row` to alter each row before insertin into the partition, but usually only for setting the ``gvid`` column to a geographic geoid based on other values in the row. In many cases, neither of these methods are required, and the Loader bundle has a nearly empty :file:`bundle.py` file.
 
Todo
****

* Tutorial process
* Intuit, raw row, and specd-row reports. 
* Meta phase
* mangle_column_name()
* build_modify_row()
* Running Bambry info
* protoschemas
* column_map



             
