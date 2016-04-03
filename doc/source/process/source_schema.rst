Source Schemas
==============

Source Schemas
--------------


Column Maps
***********

Generally, colmap editing has these steps:

- Ingest, to setup the source tables
- bambry colmap -c, to create the table colmaps
- edit the table colmaps
- bambry colmap -b, to create the combined column map
- Review the combined column map
- bambry colmap -l, to load the combined colman into the source schema.
- bambry sync -r, to sync from the source schema records back out to files.

Start by importing the headerstypes bundle from `ambry/test/bundle_tests/ingest.example.com/headerstypes`.  This bundle
has one file, header4, that has an extra header line, so the column names are a bit off.

.. code-block:: bash

    $ ambry import -d ambry/test/bundle_tests/ingest.example.com/headerstypes
    $ bambrycd d00headers003
    $ mv schema.csv schema-orig.csv
    $ mv source-schema.csv source-schema-orig.csv

Creating the Column Map
+++++++++++++++++++++++

Ingest the header files. Then create a column map file and open it in a spreadsheet program.

.. code-block:: bash

    $ bambry ingest -t headers
    $ bambry colmap -c
    $ open colmap_header.csv.

[ TO make these descriptions easier, 'column' referrs to a colum in the spreadsheet, while a column in a source or
destination table will be called a 'field' ]

In the colmap file, the first column is the names of the fields of the destination table, and the rest of the
columns are for the field names in an ingested source. In the :file:`colmap_header.csv` file, you'll notice that
most of the files have the same field names in the same rows, but `headers4` is off in its own.

The file can be a bit hard to understand, so think of it like this: Find a column, column 2 or larger, where the first
line is the name of a source. Then, go down that column to find a particular field name in the source. Then, look at the
name in the first column to find the field name that the source field will be mapped to in the destination.

When the column map is first created, the soruce and destination field names are all the same. In this case, The
`renter_cost_gt_30` field in the `headers1` source table maps to the `renter_cost_gt_30` field in the destination
table. However, in the `headers4` source table, the `gvid` field is `gvid_a`, and the
`title_right_in_the_middle_cost_gt_30_cv_b` is too long.

To fix these, move the `gvid_a` field name up to the row where `gvid` is in the first column, under `headers`. This
 will cause the `gvid_a` source field to be mapped to the `gvid` output column.

For this example, just move all of the `headers4` fields up, then delete the corresponding field names in the first
column. Refer to :file:`source-schema-orig.csv` to see the final version.

**Only edit field names in the first column. Outside of the first column, only move field names. Never edit the first
row.**

Creating the Column Map
+++++++++++++++++++++++

After editing one or more table column maps, you can combine them with

.. code-block:: bash

    $ bambry colmap -b

This command will create the :file:`colmap.csv` file, which will hold the source and destination field names for only the fields that changed.

Load the Column Map
+++++++++++++++++++++++

.. code-block:: bash

    $ bambry colmap -b

Load the column map into records in the bundle, altering the existing source table records with the changed from the
:file:`colmap.csv`  file.

Export the final source schema.
+++++++++++++++++++++++++++++++

.. code-block:: bash

    $ bambry sync -o

Sync out to create the new :file:`source_schema.csv` file, which has the changes you made to the `headers4` table.



Destination Schemas
-------------------




