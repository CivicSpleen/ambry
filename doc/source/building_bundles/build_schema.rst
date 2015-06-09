.. _build_schema:


In the :ref:`last section <configuring>` we configured the sources metadata to properly extract data from excel files, allowing the automatic generation of the schema. Now we can edit the schema to make the metadata more useful. 

Remapping Columns
=================

The :command:`bambry meta` command will attempt to automatically create a schema, in :file:`meta/schema.csv` and a column map, in :file:`meta/column_map.csv` 

The column map allows for changing column names before they are used to create the schema, and when two sources are used to fill the same table, but they have different column names for the same column, you can map the different source column names to the same output name. 

In the USDA example, many of the datasets have two-charaster state names, and the one for Idaho is 'id', which conflicts with the built-in id column. ( If you build the table now with :command:`bambry build --clean`, you'll get a SQL error because of duplicate primary keys. ) So, edit the :file:`meta/column_map.csv` file to map the `id` column to `idaho`

After editing the column map delete the :file:`build/schema.csv` file and re-run :command:`bambry meta --clean`. Deleting the file ensures that the tables are properly re-generated with the new column names. 


Editing Schemas
================

Once the table schema is created, you can edit the :file:`build/schema.csv` file. The most likely things that will need to be changed are the descriptions

At this point, you should be able to run :command:`bambry build --clean` and have the build complete, although it will complete with Cast errors. ( If you get a "Failed to find space identifier 'US' " error, run :command:`ambry search -I` to build the geographic identifier index )

.. code-block:: bash

    $ bambry build --clean 

When this completes, you can view your bundle data:

.. code-block:: bash

    $ sqlite3 build/ers.usda.gov/agg_product-0.0.1/farm_output.db
    sqlite> select * from farm_output limit 2; 1|1948|0.393974971552671|0.452369564170535|0.516754543840232|0.530573943902273|0.144858275521033|0.37928796099577|0.510986437586835|0.443768859084032|0.137418832051219|0.330545858608854|0.373015356981542|0.166380139270261|0.186495305587978|0.940423999602609|1.14439225308512|0.616195743106905|0.782837063702015|1.33275857019168|0.548270308784581|4.1331588594473|3.19858100270404|4.69171707094048|0.450013465364225|0.485693383111727|0.729238661712037|0.348322411055621|0.0442563697143416|0.470092130416624|0.279300837388794|0.418933344660654 2|1949|0.392209606959556|0.465019029142635|0.55293933815759|0.554340408107555|0.165486847101891|0.36723885206368|0.438895301364365|0.408050463546692|0.1314243995979|0.372555008132277|0.329373784883303|0.175732208692112|0.180102182093474|0.963639368531656|1.16678526296638|0.72626883895094|0.81716997345204|1.33731653886684|0.593194566554745|4.03355028116729|2.97175222248167|4.66688160805454|0.476314733474816|0.514910716393866|0.807311374641467|0.378178175023767|0.0530408279199408|0.469768899452822|0.32647450693304|0.407008700316161
    sqlite>

Column Prototypes
================

Column prototypes are Object Number references to columns in other datasets that identify the type of column. The prototypes are also associated with table indexes, making it easier to declare the index table that a dataset should link to. 

Column prototypes can be specified with any column id or vid, but there is also a set of well-known names that automatically expand to the identifiers for common types. These names listed in the `civicknowledge.com-proto-proto_terms` partition. 

===========================  ============  ================================  =========
name                         obj_number    index_partition                   Purpose
===========================  ============  ================================  =========
dates.iso_date               c00102002     civicknowledge.com-time-dates
dates.year                   c00102003     civicknowledge.com-time-years
dates.month                  c00102004     civicknowledge.com-time-months
dates.iso_week               c00102006     civicknowledge.com-time-weeks
censusareas.gvid             c00104002
censusareas.year             c00104003
geometries.gvid              c00105002
geometries.year              c00105003
regions.gvid                 c00106002
divisions.gvid               c00107002
states.year                  c00108002     census.gov-index-states
states.gvid                  c00108003     census.gov-index-states
counties.year                c00109002     census.gov-index-counties
counties.gvid                c00109003     census.gov-index-counties
cosubs.year                  c0010a002     census.gov-index-cosubs
cosubs.gvid                  c0010a003     census.gov-index-cosubs
places.year                  c0010b002     census.gov-index-places
places.gvid                  c0010b003     census.gov-index-places
uas.year                     c0010c002     census.gov-index-uas
uas.gvid                     c0010c003     census.gov-index-uas
tracts.year                  c0010d002     census.gov-index-tracts
tracts.gvid                  c0010d003     census.gov-index-tracts
blockgroups.year             c0010e002     census.gov-index-blockgroups
blockgroups.gvid             c0010e003     census.gov-index-blockgroups
blocks.year                  c0010f002     census.gov-index-blocks
blocks.gvid                  c0010f003     census.gov-index-blocks
zips.year                    c0010g002
zips.gvid                    c0010g003
us_addresses.county_gvid     c0010h007
schooldistricts.year         c0010i002
schooldistricts.gvid         c0010i003
===========================  ============  ================================  =========

Column prototypes are set in the `proto_vid` field in the schema file. When the schema is processed, the names above will be processed by:
- The name in the `proto_vid` column will be replaced with the object number
- The foreign_key column will be set to the table id of the main table in the named partition. 