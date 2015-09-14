.. _warehouse_planning:


Planning A Warehouse
====================

Most data users will connect to existing warehouse, but sometimes it is better to create a new one. The most common reasons are to support a data-driven website and to provide data for a group of analysts. 

In both cases, the larger the warehouse, the more planning and development will be involved in creating it. To create a large warehouse, you will: 

- Identify the outputs that will be presented to the user
- Segment the output into collections
- Identify the datasets and partitions you'll want in the warehouse
- Create output views for the data that will be presented to users. 

Identifying Outputs
*******************

Figure out what you want to show to users, what charts graphs and tables. It is very valuable to start the process with either a website design or a research question. 


Segment the outputs into collections
************************************

Break the data outputs into logical collections. It is best if the system design has an organization that can be used to identify collections.

Add the outputs to the collections as VIEWs. At this point, the views will not be defined, but you can add documentation to describe them. 

Identifying Data
****************

The first step in constructing a warehouse is to identify the data the warehouse will contain, usually by collecting partition names from the  `Ambry documentation website <http://data.civicknowledge.com/>`_.

Create a list of partitions that will be included in the data. 

While identifying the partitions, associate the partition name with the collections that it should be part of. A partition can be included in more than one collection. 

Create Output Views
*******************

After establishing collections, you can create definitions of the output views that were declared in step x. This involves creating SQL code, and possibly adding auxiliary partitions. 

Install to a Warehouse and Extract
**********************************

Install the collections to a warehouse, and possibly extract the warehouse to another data store, such as files or MongoDB. 



