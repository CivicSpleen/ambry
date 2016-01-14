Bundling a Bundle
=================

Building a new bundle can be very simple, but bundle development nearly always invovles multiple steps that are organized around the major development phases. There are a set of steps that are only executed once, when the bundle is being constructed ( The meta phase ) and steps that are executed whenever the bundle build process is being run. 

- Meta Phase

	- Create a new bundle in the library
	- Setup and ingest sources
	- Create source and destination Schemas
	
- Build Phase: Build the bundle
- Package and check it the library

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

To control this process, bundle wranglers will create a set of input files in the bundle directory. These files are:

- :file:`sources.csv`. Specified the URL, encoding and format for each of the upstream sources
- :file:`source_schema.csv`. Schema for the soruce files. Usually generated automatically, but occasinoally hand edited
- :file:`schema.csv`. The destination schema for each of the destination tables. 

Additional meta data and process information is stored in: 
- :file:`bundle.yaml`. The main configuration file, which may include modifications to the pipeline
- :file:`bundle.py`. Primary bundle class, which may include transformation functions for editing rows during processing.

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


Create a New Bundle
-------------------


Add and Ingest a Source File
----------------------------

Add the source file

Sync and ingest. 

View the ingested file


Create Schemas
--------------

Create the source schema

Create the destination schema


Test Building the Partition
---------------------------


Building
********

Build a single source

Build a single table

Limited run build

Multiprocess build





















