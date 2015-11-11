Configure a Schema
==================

Source Schemas
--------------


Column Maps
***********

Generally, colmap editing has these steps:

- Ingest, to setup the source tables
- bambry colmap -c, to create the table colmaps
- edit the table colmaps
- bambry colmap -b, to create the combined column map
- Review the combined column map
- bambry colmap -l, to load the combined colman into the source schema.
- bambry sync -r, to sync from the source schema records back out to files.

Destination Schemas
-------------------