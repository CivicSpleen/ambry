.. _about_overview:


Overview
========

*Ambry Data Bundles are packages of data that simplify the process of finding, cleaning, transforming and loading popular datasets.*

The data bundle format, tools and management processes are designed to make common public data sets easy to use and share, while allowing users to audit how the data they use has been acquired and processed. The Data Bundle concept includes the data format, a definition for bundle configuration and meta data, tools for manipulating bundles, and a process for acquiring, processing and managing data. The goal of a data bundle is for data analysts to be able to run few simple commands to find a dataset and load it into a relational database.

Data Bundles are designed for high-value public datasets, such as census, economic reports, crime data, records of contracts, and other public data sets that many people use on a regular basis. For these data, it is important to be able to verify where the data originated and understand how the data was processed. Different users may require the same original data to be post processed in different ways, and occasionally, data may be corrected or updated. Unlike a traditional data warehouse environment, Data Bundles are designed for an environment where instead of a "single version of the truth" user want to know which version of the truth they have.


Requirements
************

Data bundles are designed for a particular use that involves these high level requirements:

* Installation to a relational database. Data in a bundle is considered installed when it has been loaded into a relational database.
* Installation of a subset of data. Some data sets are very large, on the order of 100GB, so users must be able to install only part of a set.
* Linked sets. Installation of one set may require another set to be installed.
* Revisions. Sets can be updated, usually to fix errors, so it must be possible to reload a set in a database
* Variations. Different uses require different models for the same data, so users must be able to pick from multiple variations of the same underlying data
* Formal Column Metadata. Users must be able to easily find sets that can be linked to other sets, so columns must be marked with enough data to determine if two columns in different sets are comparable or linkable.

If you are familiar with Business Intelligence and Data Warehousing practice, data bundle relate to ETL by separating ET from L: Data is extracted and packaged into bundles, and the bundles can be loaded into a warehouse separately.


Example Scenario
****************

An example of the motivation for these requirements is the US Census, which is one of the most important sets that will be packaged in a data bundle. The 2000 US Census consist of 9 subsets: 4 Summary Files, 3 Congressional District files,  a 5% sample demographics set, and a 1% demographics set. Uncompressed in CSV format, Summary File 1 is about 80GB, with 9.5M rows and 238 tables holding 8,000 columns. It is unreasonable to expect even expert users to download 80GB of data, so this file must be partitioned to be usable.
While the census data is interesting by itself, it is most useful when linked to other sets. In particular, it is common to link census data to the geographic areas for which the data was collected.  This information is contained in the TIGER/Line shape files, which also has many subsets.

Since both the Census data and the TIGER shape files have  a lot of variables, users would like to be able to ask the package system to return only the data for particular variables. For instance, a user who wants to compute the density of homeowners per county should be able to search for "Home owner", "county" and "area" and get back the correct census subsets with the TIGER/Line shape-file sets that link to the census tables and have an area variable.
All of the files that are packaged in Data Bundles are subject to errors, so it must be possible to update a set, which requires a method of making revisions. But, Data Bundles can also includes data that is modeled for a particular kind of analysis.

For instance, Census files present the ages of the population in a set of 60 or more columns, with each column representing either a single age or a range of ages. Some users will frequently use particular age ranges, such as 0-18, 0-21, and 21+.  For these users, a bundle can include an additional table that pre-computed these alternative summaries. Because the bundle can be designed for particular users, there may be different versions of the US Census. Users will need a way to search for these different variations.

