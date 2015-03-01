.. _warehouse_overview:

Warehouse Creation Overview
===========================

Ambry warehouses are collections of datasets that are organized around a theme or project. The warehouse is the last step in a data management process that begins with Bundles. 

In Ambry, data is packaged into Bundles, with each Bundle being composed of one or more partitions. The partitions are
segmented along natural divisions in the data. For instance, with education data, there is usually one partition per
year, while for Census data, each year is a separate bundle, and there is one partition per state. The partition is a
single-file relational database ( a Sqlite database ) and usually contains only one table.

Bundles and Partitions are stored in Libraries, web-based collections of data files. Once bundles are loaded into a
Library, the user can query the library to look for bundles and partitions. After finding interesting partitions, the
user can install the partitions to a warehouse, which is a relational-database where the data from the partitions can
be accessed through a SQL connection to the database, or though file extracts, accessed through a web application.

This overview covers the structure of a warehouse and how to use them.

The Process
-----------

The full process for getting data into a warehouse involves:

-  Building data Bundles, one for each source dataset
-  Grouping the Bundles into Collections
-  Installing the Collections into a relational database

Building Bundles
----------------

The process starts by collecting data from the data producer and creating a Bundle. The Bundle starts with a
configuration file, a database schema and a program. Running the program creates a set of database files that hold the
data that was downloaded from the source. Creating bundles ensure that all of the data we use is in the same format, is
properly documented, and can easily be installed into a database.

Here is an example of a bundle that we use for testing, `which you can view on Github`_. The important files in this
bundle are the `configuration file,`_ which defines important metadata, the `schema.csv file`_, which describes all of
the tables and columns in the bundle data, and \ `the bundle program`_, a Python language program that creates the
database files from the source files.

Here is an example of a bundle that we use for testing, `which you can view on Github`_. The important files in this
bundle are the `configuration file,`_ which defines important metadata, the `schema.csv file`_, which describes all of
the tables and columns in the bundle data, and \ `the bundle program`_, a Python language program that creates the
database files from the source files.

After the bundle is built, it is installed in the library, and documentation pages are created. The `documentation for
the example file`_ is a bit sparse, but other bundles have `more complete documentation`_.

Creating Collections
--------------------

Bundles can be assembled into collections with a manifest file. The collection links several bundles together around a
theme, and declares database views to create new ways to organize the data. For instance, the `CA Health collection
manifest`_ links files from OSHPD, The CDC and the US Census, and then creates a database view that tailors these
datasets to a particular use.

Installing Collections
----------------------

To use a collection, it is installed in a warehouse database. This will create the database if it does not exist, and
add entries to the library documentation. After installing a collection, you can `browse the collection
documentation`_, or view the\ `documentation for the`_\ warehouse that the collection was installed into. There are
separate entries for the collection and the warehouse because multiple collections can be installed into a single
database.

Because the warehouse database has all of the data in it, the `documentation site`_ can generate download files, making
it easy for users to get access to CSV or JSON versions of data tables. Additionally, users can get the `JSON format
description of tables`_ and views. These JSON files are complete descriptions of the table, all of the columns, and the
bundles that provided the columns and tables. Using these JSON files, users can, for instance, link any column in any
table to the documentation for

.. _which you can view on Github: https://github.com/CivicKnowledge/ambry/tree/master/test/bundles/example.com/random
.. _configuration file,: https://github.com/CivicKnowledge/ambry/blob/master/test/bundles/example.com/random/bundle.yaml
.. _schema.csv file: https://github.com/CivicKnowledge/ambry/blob/master/test/bundles/example.com/random/meta/schema.csv
.. _the bundle program: https://github.com/CivicKnowledge/ambry/blob/master/test/bundles/example.com/random/bundle.py
.. _documentation for the example file: http://data.civicknowledge.com/bundles/dHSyDm4MNR002.html
.. _more complete documentation: http://data.civicknowledge.com/bundles/d030001.html
.. _CA Health collection manifest: https://github.com/CivicKnowledge/collections/blob/master/health/ca_health.ambry
.. _browse the collection documentation: http://data.civicknowledge.com/manifests/mImXqL1Uho.html
.. _documentation for the: http://data.civicknowledge.com/stores/sgO5nW6Ymo.html
.. _documentation site: http://data.civicknowledge.com/stores/sgO5nW6Ymo.html
.. _JSON format description of tables: http://data.civicknowledge.com/stores/sgO5nW6Ymo/tables/t0000j001.json


