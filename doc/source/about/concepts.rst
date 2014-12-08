.. _about_concepts:

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






