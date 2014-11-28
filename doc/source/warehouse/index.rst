.. _warehouse:

Creating and Using Warehouses
=============================

In Ambry, a warehouse is a relational database in which Ambry partitions have been installed. While an Ambry Library is a disconnected collection of database files, a Warehouse allows data tables to be linked, querried and used to create new views and tables. 

Most users will interact with a warehouse through a SQL connection, directly issuing SQL commands, or will use the warehouse's web interface to download extracts of tables as files. 

Some of the important features of an Amby Warehouse include: 

- Implemented in a relational database, usually Postgres, MySQL or Oracle. 
- Supports geographic queries, when the database is Postgres ( via PostGIS )
- Each warehouse has a documentation web application that shows all installed partitions, tables and table columns
- Users can download tables and views in a wide variety of file formats, including CSV, KML, JSON, GeoJSON and others. 
- Warehouse tables can be exported to NoSQL databases like MongoDB. 

Loading Data into a Warehouse
*****************************

To load a warehouse, the user creates one or more `Manifests`. Like its namesake, a list of the contents of a ship, a Manifest is a list of partition, with associated documentation and SQL views to make it easer to use the data. When installed, the data from a Manifest is usually called a Collection.
Here is what a simple manifest looks like:: 

	TITLE: A Health Manifest

	This is a fake education file manifest

	UID: mQaiWWgy2G
	AUTHOR: Eric Busboom <eric@sandiegodata.org>
	DATABASE: sqlite:////example_manifest.db
	

	PARTITIONS:
	example.com-segmented-example-2
	example.com-combined-example

	VIEW: health_view
	SELECT * FROM "pIjqPRbrGq001001_example"

	VIEW: health_mview
	SELECT * FROM "p00G002001_example"

	EXTRACT: health_view AS csv to health_view.csv



This manifest includes a title, a description, two partitions, two views and one extracted file. The manifest also specifies which database data should be written to, and where to send extracted files and documentation. This manifest specifies installation to a Sqlite database, but that specification is only used when no other database is specified during installation; the manifest can be installed to other databases as well. 

This manifest can be installed from the command line with:

.. code-block:: bash

	$ ambry warehouse install example.ambry

The result is a Sqlite database, ``example_manifest.db``,  which contains two sets of tables. The first set is the Library, which keeps track of what data is installed. These are exactly the same tables are are in an Ambry Library.

- datasets
- partitions
- tables
- columns
- files
- config

These tables store all of the information about the installed data, and can be used to generate complete documentation.

The second set of tables are the installed partition tables. These tables are copied directly from the partitions listed in the ``PARTITIONS`` section:

- p00G002001_example
- pIjqPRbrGq001001_example


There are also a few views installed. The first two are created automatically, and the second two are defined in the Manifest:

- pIjqPRbrGq001_example
- p00G002_example
- health_view
- health_mview

The installed data tables are named with the table name, ( in this case, always "example" ) and the partition id, which is the prefix to the table names. The views are given the same name as they have in the ``VIEW`` statements in the manifest, but they can also have the Manifest ``UID`` as a prefix.

A ``VIEW`` is a normal SQL view, while an ``MVIEW`` is a sort of materialized view, which is actually a table that is loaded from the query defined in the ``MVIEW`` section.

The example tables are installed as a table with the full partition id, including, as the last three digits of the prefix, the version number.  Then, the warehouse also creates a view that refers to the table, with the view name excluding the version. This allows users to use the name without a version number to create SQL queries, so that installing a later version of the same partition will not require the SQL query to be altered. The definitions of health_view and health_mview demonstrated using the table for a partition and the associated view.

Documentation
*************

Each library and warehouse has a documentation website that list all of the packages it contains.

The documentation server runs of  cached JSON files, which can be viewed as JSON by changing the file on any page, from a ".html" extenstion to ".json".

SQL Access
**********

Warehouses are relational databases, and users can connect directly to a warehouse to issue SQL commands. Use the documentation to understand the structure of the database

File Access
***********

The documentation website lists the Collections that were created from the Library, and which databases those collections are installed in. These collections can include EXTRACT commands to generate files. These files are accessible thought the documentation website.

Every tables can be accessible through the documentation server, not just extracts. Maybe the extracts are the ones that are advertised, but users can probably get file access to anything in the warehouse.

There are two general forms of the ``EXTRACT`` command. The first, which is shown in the example manifest, extracts to a specific file format and file name:: 

	EXTRACT: health_view AS csv to health_view.csv
	
This will extract all of the rows in the ``health_view`` view to a CSV file. The file, named ``health_view.csv`` will be placed in a directory with a path that is based on global configuration and the ``UID`` of the manifest. Users can have files extracted during the installatoin of the manifest, or later, with a seperate cli command. 

The second form is more general, omitting the format and file name:: 

	EXTRACT: health_view
	
This can also be done with a ``-e`` parameter on the view:: 

	VIEW: health_view -e
	
This form of extract is not performed automatically during installation, but can be triggered by users through the documentation inteface. The web interface allows the user to select the format of the file, and the download name is based on the name of the view. While viewing the documentation page for the manifest, the extract will be marked with a "Download" button that when pressed, allows the user to select a download format for the file. The user can also copy a link to use in other applications. 

Views that are marked this way can also be extracted, in bulk, to other data stores.


MongoDB Extracts
****************

A particular example of running a bulk extract process is targeting a MongoDB database, which will create a MongoDB table for every general extract entry. 

Extracting to a MondoDB database will also generate a set of tables for the library, creating one document for each of the JSON files created for use in the documentation web application.



