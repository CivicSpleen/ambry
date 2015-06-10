.. _configure_schema:

Configure Schema
=================


In the :ref:`last section <configuring_sources>` we configured the sources metadata to properly extract data from excel files, allowing the automatic generation of the schema. Now we can edit the schema to make the metadata more useful. 

Remapping Columns
*****************

The :command:`bambry meta` command will attempt to automatically create a schema, in :file:`meta/schema.csv` and a column map, in :file:`meta/column_map.csv` 

The column map allows for changing column names before they are used to create the schema, and when two sources are used to fill the same table, but they have different column names for the same column, you can map the different source column names to the same output name. 

In the USDA example, many of the datasets have two-charaster state names, and the one for Idaho is 'id', which conflicts with the built-in id column. ( If you build the table now with :command:`bambry build --clean`, you'll get a SQL error because of duplicate primary keys. ) So, edit the :file:`meta/column_map.csv` file to map the `id` column to `idaho`

After editing the column map delete the :file:`build/schema.csv` file and re-run :command:`bambry meta --clean`. Deleting the file ensures that the tables are properly re-generated with the new column names. 

Editing Schemas
***************

Once the table schema is created, you can edit the :file:`build/schema.csv` file. The most likely things that will need to be changed are the descriptions.

After configuring the schema you can :ref:`build the bundle <build_bundle>`.
