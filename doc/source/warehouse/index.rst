.. _warehouse:

Creating and Using Warehouses
=============================




Loading Data into a Warehouse
*****************************


To load a warehouse, the user creates one or more Manifests. Like its namesake, a list of the contents of a ship, a Manifest is a list of partition, with associated documentation and SQL views to make it easer to use the data. When installed, the data from a Manifest is usually called a Collection.
Here is what a simple manifest looks like:: 

	TITLE: A Health Manifest

	This is a fake education file manifest

	UID: mQaiWWgy2G
	AUTHOR: Eric Busboom <eric@sandiegodata.org>
	DATABASE: sqlite:example_manifest.db
	LOCAL: example_manifest
	REMOTE:  s3://warehouse.sandiegodata.org/test/example_manifest#compress

	PARTITIONS:
	example.com-segmented-example-2
	example.com-combined-example

	VIEW: health_view
	SELECT * FROM "pIjqPRbrGq001001_example"

	VIEW: health_mview
	SELECT * FROM "p00G002001_example"

	EXTRACT: health_view AS csv to health_view.csv


This manifest includes a title, a description, two partitions, two views and one extracted file. The manifest also specified which database data should be written to, and where to send extracted files and documentation.

This manifest can be installed from the command line with:

.. code-block:: bash

	$ ambry warehouse install example.ambry

The result is a Sqlite database, ``example_manifest.db``,  which contains two sets of tables. The first set is the Library, which keeps track of what data is installed. This is exactly the same tables are are in an Ambry Library.

- datasets
- partitions
- tables
- columns
- files
- config

These tables store all of the information about the installed data, and can be used to generate complete documentation.
The second set of tables are the installed partition tables:

- p00G002001_example
- pIjqPRbrGq001001_example
- health_mview

There are also a few views installed:

- pIjqPRbrGq001_example
- p00G002_example
- health_view

The installed data tables are names with the table name, ( in this case, always "example" ) and the partition id, which is the prefix to the table names. The views are given the same name as they have in the ``VIEW`` statements in the manifest, but they can also have the Manifest ``UID`` as a prefix.

A ``VIEW`` is a normal SQL view, while an ``MVIEW`` is a sort of materialized view, which is actually a table that is loaded from the query defined in the ``MVIEW`` section.

The example tables are installed as a table with the full partition id, including, as the last three digits of the prefix, the version number.  Then, the warehouse also creates a view that refers to the table, with the view name excluding the version. This allows users to use the name without a version number to create SQL queries, so that installing a later version of the same partition will not require the SQL query to be altered. The definitions of health_view and health_mview demonstrated using the table for a partition and the associated view.

Documentation
*************


Each library and warehouse has a documentation website that list all of the packages it contains.

SQL Access
**********

Warehouses are relational databases, and users can connect directly to a warehouse to issue SQL commands.

File Access
***********


The documentation website lists the Collections that were created from the Library, and which databases those collections are installed in. These collections can include EXTRACT commands to generate files. These files are accessible thought the documentation website.
Every tables can be accessible through the documentation server, not just extracts. Maybe the extracts are the ones that are advertised, but users can probably get file access to anything in the warehouse.

MongoDB Extracts
****************


Ambry can target MongoDB to store extracts from a warehouse.
It's just normal extract.

Could also target the cached JSON to Mongo. It would have to include additional information about the extracts, so you could get an index of the extracts. The extract should have metadata set by the  Manifest the extract was defined in.

(star) Add a way to apply tags to an exrtact
