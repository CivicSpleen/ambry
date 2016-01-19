.. _overview:

Concepts
========


The conceptual model for Ambry is centered on the method of breaking datasets into parts, a division which is composed of two levels, the ``Bundle`` and the ``Partition``. A ``Partition`` is seperable part of a dataset, such as a single table, or a year's worth of records. A ``Bundle`` is the collection of all partitions that make up a dataset.

Both bundles and partitions are differentiated on one or more of several dimensions, such as time, space or table. This document will describe the components and how they fit together. 

Conceptual Model
****************

- :ref:`about_bundles`. A synonym for a dataset, a collection of one or more partitions of data, representing a cohesive dataset released by an upstream source. 
- :ref:`about_partitions`. A collection of related records that are part of a bundle. Partitions are differentiated from other parttions in the same dataset along one or more dimensions. 
- *Table*, a collection of columns, similar to a Table in SQL. 
- *Column*, the name and type for a series of data, similar to a Column in SQL. 

A complete library will typically have hundreds of thousands of these objects, and each object may have multiple versions, so naming an numbering is one of the most important parts of Ambry. 

- *Identity*. The unique name and number for a partition or a bundle. The identity holds an object number and a name. 
- *Name*. Bundle and partition names are concatenations of dimensions. 
- *Dimension*. A component of a name, such as the source of the dataset, a time range, the geographic extent of the data, or other distinguishing factors. 
- *Object Number*. a unique, structured number that distinguishes bundles, partitions, tables and columns. 

For the details of numbering and naming, see :ref:`about_numbering`.

.. _about_bundles:

Bundles
*******

A ``Bundle`` stores a complete dataset, combining multiple datafiles with documentation. While many datasets are provided by the upstream source as a single CSV file, there are also datasets that are composed of hundreds or thousands of files. An Ambry Bundle is designed to handle datasets ranging from a list of counties to the US Decennial Census. 

The primary division in storing data is the``Partition``. Every bundle must have one partition, but may have hundreds or thousands. Bundles also hold ``Tables``, which are linked to ``Partitions``

Besides the data, one of the most important parts of the biundle it's metadata, which include information that defines how to build the bundle, as well as documentation, tables schemas, contact information for getting help with the data, URLs to data dictionaries, and much more. 


.. _about_partitions: 

Partitions
**********

A ``Partition`` is a container for data. Small Bundles, with only one table, less than 5M rows and no conceptual divisions, will be stored in a single partition. The US Census, on the other hand, has one partition per state and per table, for a total of several thousand partitions. 

Partitions are organized according to a natural divisions in the data. For a state dataset that is updated per year, there is likely to be one partition per year. For a national dataset with multiple tables, but which never updated after release, the division may be by state. 

The dimensions along which partitions may be divided are: 

- Time. A time period, expressed in ISO format. 
- Space. A Census geoid or other formal identifier for a political area.
- Grain. A name for the smallest unit of aggregation, such as "Tract" or "School"
- Segment. A sequential number, for when large tables are broken into multiple partitions. 
- Table. The name of the primary table stored in the partitions. 


.. _about_numbering:

Object Numbering
****************

As with any system that stores a lot of objects, a well-defined naming system is critical to data management systems. Ambry uses a naming system that employees unique id number as well as human-readable names.



Ambry uses structured Base-62 strings to uniquely identify objects in the system. These numbers frequently appear as prefixes to table names and similar places, so it is important to keep them short; a typical UUID would have been too long. Because the numbers are short, there is a more limited space for allocation, which requires a centralized number server, although there is an alternative that allows users to generate longer numbers without the central authority.

Here are a few examples of Ambry object numbers:
* d00B001 A Dataset
* pHSyDm4MNR001001 A partition within a dataset
* c00H02001002 A column identifier, with a version number

The objects that Ambry enumerates are:

* Bundles, also called Datasets. Prefix is 'd'
* Partitions, a part of a Bundle. Prefix is 'p'
* Tables, a part of a Bundle. Prefix is 't'
* Columns, a part of a Table. Prefix is 'c'

Because all of these objects are related, the partitions, Tables and Columns all have numbers that are based on the number of the BUndle the object is part of.

All of the numbers are integers expressed in Base-62, which uses only digits and numbers. Using Base-62, rather than Base-64, makes it easier to use the numbers in URLs without encoding.

  bdigit            = ALPHA / DIGIT

  bundle_seq        = ( 3bdigit / 5bdigit / 7bdigit / 9bdigit )

  bundle_number     = "d" bundle_seq

  partition_seq     = 3bdigit

  partition_number  = "p" bundle_seq partition_seq

  table_seq         = 2bdigit

  table_number      = "t" bundle_seq table_seq

  column_seq        = 3bdigit

  column_number     = "c" bundle_seq table_seq column_seq

  revision          = 3bdigit

  object_number     = ( bundle_number / partition_number / column_number
                      table_number ) [revision]


There are four lengths for the bundle sequence: 3,5,7 or 9 digits, one for each 'registration class'. The registration class is roughly the level of acess a user has to a central numbering authority.

* authoritative. 3 Characters. Reseved for a designated registration authority.
* registered. 5 characters. For users that have created an account at the numbering server.
* unregistered. 7 Characters. For users that use the registration authority, but havent' created an daccount.
* self. 9 Characters. A random number assigned locally.

The 3, 5 and 7 digit numbers are assigned by a central authority, so the number space is dense. ( 5 Base-62 digits is approximately 1 billion numbers. ) The 9 digit numbers are self assigned and are chosen randomly.

All bundles have a revision, and the bundle's revision number is used for all of the Bundle's objects. However, the revision is optional in many cases, such as when referencing an object with respect to a specific bundle, or when only one version of a bundle is installed in a database.

Because of these variations, object numbers can have a variety of lengths. Bundle numbers, for instance, can have lengths generated by : 1 + ( 3 | 5 | 7 | 9) + ( 0 | 3)  = 4, 6, 7, 8, 9, 10, 11 or 13 characters. The base set of lengths, (3, 5, 7, 9) were chosen to ensure that all of the permutations of lengths are unique, allowing the type of number to be determined from the length without knowing beforehand whether the number has a version or not.


Object Names
************

Names are human-readable strings that are composed of individual components, called Dimensions. The dimensions are stored seperately in the bundle's configuration. The names for these dimensions are:

* source. The Domain name of the origin of the datafile
* dataset. A name for the dataset
* subset. A name for a sub-component of the dataset
* bspace. A name for the geographic region that encompases the data. ( The name differentiates if from  the _space_ component of partitions )
* btime. An ISO designator for the time interval of the dataset. ( The name differentiates if from  the _time_ component of partitions )
* variation. A name for a variant of an earlier dataset. The value 'orig' means that the bundle is the first import of the data from the source.
* version. A semantic version number

The _btime_ component should be formated according to ISO8601, with one variation. For instance:

* "2005". All data is for the year 2005.
* "200610". All data is for the month of October, 2006.
* "200601P3M". The first quarter of 2006.
* "P5YE2010". The 5 year period ending in 2010. ( The 'E' is a non-standard substitution for '/')

Some examples of names include:

- cccco.edu-wageoutcomes-summary
- census.gov-2010_population-geo-orig-0.1.6
- census.gov-2010_population-geo-orig-geofile-50


Fully Qualified Names and Identities
************************************

Names and Numbers are occasionally seen combined in an ``fqname``, a Fully Qualified name. These are simply a versioned name and the object number of a dataset, combined with a '~' character::

	cccco.edu-wageoutcomes-0.0.1~d02l001

An ``Identity`` is a code object that combines all of the individual components of the name and number, for bundles and partitions, and permits them to be manipulated.


.. _building_overview:

Bundling a Bundle
*****************

Building a new bundle can be very simple, but bundle development nearly always invovles multiple steps that are organized around the major development phases. There are a set of steps that are only executed once, when the bundle is being constructed ( The meta phase ) and steps that are executed whenever the bundle build process is being run. 

The meta phase involves: 

	- Create a new bundle in the library
	- Setup and ingest sources
	- Create source and destination Schemas
	
The build phase has one cannonical step, running the build, but in practice, it involved debugging the configuration and maching changes to improve the quality of the bundle. 

After the bundle is built, you can use it, or check it into a library for other people to use. 

Most of the effort is in the meta phase; if the bundle is constructed properly, the build phase runs  without intervention. 


How Bundles Build
*****************

To understand how to construct a bundle, you should first understand how the build process works. 

The goal of the build process is to construct a partition file, a file that hold a portion of the data in the dataset. Each partition is created from upstream source data, which is defined in the :file:`sources.csv` file. The sources are usually downloadable files, but also can be python code or SQL queries.

So, at the top level, data flows from the upstream source to a partition. 

However, the mapping is not direct; one upstream source can feed into multiple partitions, and multiple sources can feed into a single partition. The build runs through each upstream source, selects all or just some of the rows in the source, and puts them into a partition segment file. Then, at the end of the process, the segment files are coalesced into a partition. 

For example, if you have these sources: 

- source1
- source2

And the selection process splits them into even and odd rows, the middle stage of the process would result in these partition segments: 

- partition-even-source1
- partition-odd-source2
- partition-even-source1
- partition-odd-source2

Then, after the segments are coalesced: 

- partition-even
- partition-odd

The source files all have a schema, metadata that describes the column names and their data types. Since most source files are CSV or fixed with, the type information must be inferred, so the source schem can take some effort to construct. However, it is vital to do so, because the column names in the source files must match with column names in the destination schemas in the partitions. 

Each source file has its own source schema, so in our example, there would be source tables for ``source``` and ``source2``. Partitions, however, may not have unique destination schemas -- they can all be associated with the same destination table. In our example, both the even and odd partitions may use the same destination table format. 

It is very common to have multiple source files that all feed into the same table. For instance, a multi-year dataset may have one file per year, so while each of the source files will have its own source schema, there would be only one destination schema. Unfortunately, its also common for the source files to have differences in their schemas, such as column names that have changed or which are named with the year in the them, In these cases, the source schema column must be mapped to a new name that will be the same for all of the files. 

The need to map column names is why there are two schemas, one for the source and one for the destination table that the partition uses. The source schema has two names for each column, an soruce name and a destination name, so the column names can be changed as the source is processed. 

The Pipeline
************

The whole process works something like this: 

- Read the upstream source file
- Possibly map the source column names to new names
- Select a partition to write each row to. Write the row to a segment file for the partition
- For each partition, create the partition by coalescing the segment files. 

This process is controlled by the Pipeline. The pipeline consists of a series of pipe, each of which has one function to process a dataset's header, and another to process each of the rows. The pipes are connected so that the upsream source is fed into the source end of the pipe, and the final pipe stage writes rows to the segment files. The source file header is fed in first, then each of the rows.

Each pipe in the pipeline is a subclass of :class:`ambry.etl.Pipe`. The default pipeline is: 

- Source pipe, dependent on type of source
- :class:`ambry.etl.MapSourceHeaders`. Applies the output column names to the upstream source
- :class:`ambry.etl.CastColumns`. Applied colum value transformations, casting to final datatypes
- :class:`ambry.etl.SelectPartition`. Determines which segment partition row should be written to
- :class:`ambry.etl.WriteToPartition`. Writes rows to a segment file. 

After all of the segments have been written for a partition, the partition is coalesced, outside of the pipeline. 

Process Summary
***************

To control this process, bundle wranglers will create a set of build files in the bundle directory. These files are:

- :file:`sources.csv`. Specified the URL, encoding and format for each of the upstream sources
- :file:`source_schema.csv`. Schema for the soruce files. Usually generated automatically, but occasinoally hand edited
- :file:`schema.csv`. The destination schema for each of the destination tables. 

Additional meta data and process information is stored in: 
- :file:`bundle.yaml`. The main configuration file, which may include modifications to the pipeline
- :file:`bundle.py`. Primary bundle class, which may include transformation functions for editing rows during processing.

.. _file_locations:

File Locations
**************

When working with these build files, it is important to know that there are three states or locations for the information in the files: 

- On the file system. The file's information can be in a normal file in the file system. 
- In a file record. When files are synced in, they are copied into a database recordss in the bundle. 
- As objects. The file records are turned into collections of objects, such as tables, columns or partitions. see the `Object Model`_ section for a descriptino of these  database objects. 

The build configuration is broken into these three levels to allow for maintaining the fidelity of  build source files -- ensuring that errors in the files don't result in them being deleted if there are errors -- while also allowing for bundles to be constructed entirely programatically, without files at all. 

An important implication of this structure is that you will frequently sync in and sync out build source files, either by using the :command:`bambry sync` command, or by using the `-y` option to :command:`bambry clean`


Object Model
************

There are many objects stored in the database for a bundle; these are the most important: 

- Dataset. The main database records for a bundle
- Partition. A collection of data rows, roughly corresponding to a single CSV file when extracted, although the Partition object only records the identity of the partition. 
- Table. A collection of columns that define the structure of data in a partition. 
- Column. A single column in a table. 
- Source. A record of the location and type of a source input file, including its URL, encoding, number of header rows, and other important information. 
- Source Table. A simpler version of the Table object, for describing the structure of an input source file. Every Source has a Source Table. 
- Source Column. A column in a soruce Table. 


Meta Phase
**********

In the meta phase, you will create the new bundle and configure the files. The steps in this phase are usually: 

- Create the new bundle with :command:`bambry new`, then export the files to a directory
- Edit the :file:`sources.csv` to refer to the input source files.
- Ingest the files with :command:`bambry ingest`, edit :file:`sources.csv` until ingestion runs smoothly. 
- From the ingested files, create the source schema with :command:`bambry schema -s`
- Possibly edit the schemas to alter column names 
- Create the destination schemas with :command:`bambry schema -d`
- Edit the destination schemas so the source files build properly







