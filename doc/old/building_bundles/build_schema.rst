.. _build_schema:

Bundling Schemas
================


Column Prototypes
*****************

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