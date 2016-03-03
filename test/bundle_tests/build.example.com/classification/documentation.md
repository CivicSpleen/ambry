
## How the data were processed

This data package combines original files that were broken into seperate Excel files, and splits files on the type of geographic grain, so each of the output files consists of only one geography, covering the whole state. For instance, the original release has four __Public_Transit_Access__ files, one each for large regions of California. These files were combined, then split again into 5 files, one each for the county, tract, place, region and CMSA geographic aggregations. 

The `geotype` field has a code the describes the type of geography used in 
aggregating each row. These geographies are generally Census geographies, 
and are specified with Census geoids, in the `geotypevalue` field. The `geotype` codes are:

- `CA` or `ST` The whole state of California
- `CO` A county
- `CD` A county subdivision
- `PL` A Census Designated Place
- `RE` A Sub-state region
- `ZC` ZCTA, the Census version of a ZIP code area.
- `R4` Consolidated Metropolitan Statistical Areas
- `MS` Metropolitan Statistical Area

These values, excluding RE, R4 and MS are converted to a GVid for 
linking to other files. 

On most files the state code is `CA`, but in the Open Space file it is `ST`.

These geotype codes are all mapped to names and used as part of the file names. 

Other important processing steps included: 
- The `ind_definition` and `ind_id`, which have a name and number for each of the indicators, were removed from the data and used as table descriptions. These value appear to be constant in all rows in a file.
- Some very large RSE values have been changed to NULL

As of Dec 1, 2015, In the ``Neighborhood Change`` files, the Relative Standard Error (rse) column is often computed for values that are very close to zero, so the RSE is very large. In other files in this dataset, the rse value is capped
at 100. As per Dulce Bustamante-Zamora at CDPH, these values should be blank, (NULL) so this correction is made for rows where the difference is 0. 

The `reportyear` field can be either a single integer year, or it may be a range of years, which is represented as a string. 