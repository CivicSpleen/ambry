.. _about_concepts:

=============
Concepts
=============

The conceptual model for Ambry is centered on the method of breaking datasets into parts, a division which is composed of two levels, the Bundle and the Partition. A *partition* is seperable part of a dataset, such as a single table, or a year's worth of records. A *bundle* is the collection of all partitions that make up a dataset.

Both bundles and partitions are differentiated on one or more of several dimensions, such as time, space or table. This document will describe the components and how they fit together. 

****************
Conceptual Model
****************

* *Bundle*. A synonym for a dataset, a collectino of one or more partitions of data, representing a cohesive dataset released by an upstream source. 
* *Partition*. A collection of related records that are part of a bundle. Partitions are differentiated from other parttions in the same dataset along one or more dimensions. 
* *Identity*. The unique name and number for a partition or a bundle. The identity holds an object number and a name. 
* *Name*. Bundle and partition names are concatenations of dimensions. 
* *Dimension*. A component of a name, such as the source of the dataset, a time range, the geographic extent of the data, or other distinguishing factors. 
* *Object Number*, a unique, structured number that distinguishes bundles, partitions, tables and columns. 
* *Table*, a collection of columns, similar to a Table in SQL. 
* *Column*, the name and type for a series of data, similar to a Column in SQL. 


Dimensions
----------

Bundle dimensions are parts of the name of the bundle. 

Bundle Dimensions:
* source. The URL of the organization that produced or publishes the data. "census.gov'"
* dataset. A common name for the data release 'Population Census'
* subset. A sub title for the dataset, when there are several data sets known byt the dataset name. 'Summary File 1'
* type
* part
* bspace. A name for the spacial extent of the data. "San Diego County"
* btime. A time, usually in ISO 8601 format, for the time range of the data "p5ye2012""
* variation. A name for a variation, such as "Cleaned" or "Orig"
* version. A semantic version number, "1.3.34"

Partition Dimensions:
* time
* space
* table
* grain
* format
* segment

