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








