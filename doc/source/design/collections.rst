.. _design_collections:

Collections
===========


Collections group together paritions with SQL views on those partitions to provide alternate ways to acess data. Collections are important for joinging multiple partitions together, restricting the records returned in extracts, altering the titles of dataset columns, and many other use cases. 

A collection is composed of: 

- One or more partitions
- SQL views using data on those partitions
- Table and COlumn records to  document the schemas for the partitions and views
- Extra metadata to document the collection and views. 

Collections are implemented as a special case of datasets. Each partition  that is addes to a collection is  recorded as a new partition record, with the same  identity, but a ``ref`` that points to the original partition. Tables from the partitions are also given new table records, which have altered column names. Views have a new partition record to link to the partitions that were used to construct the view, and a new table record to hold all of the columns created in the view. 

Specifically, there are three kinds of data objects in a collection:
  - A normal partition, copied in its entirety from a library partition. 
  - A view, which is a normal SQL view, usually linking together one or more partitions. 
  - An indexed view, which links multiple partitions to an index partition
  - a materialized view, a table generated from a view, that reduces the cost of running expensive queries. 
