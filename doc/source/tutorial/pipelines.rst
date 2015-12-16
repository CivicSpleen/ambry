Configure the Pipelines
=======================

The pipeline is...

Piplines can be modified in code, or in the bundle.yaml configuration. 

In cofiguration, the pipeline is named based on the phase, stage and table. For phase 'phase', source 'source',  source table 'sourcetable' and destination table 'desttable', Ambry will look for piplines in the configuruation under these names, and in this order: 

* phase-sourcetable
* phase-desttable
* phase-source
* phase

Pipeline Segments 
-----------------

The pipeline is composed of segments, each of which holds pipes. The segments make it easer to insert pipes in the middle of the pipeline, because you can append or prepend a pipe to any segment. 

The segments are: 
* source: Use internally for setting the source of rows. 
* first: The first segment after the source
* body: The main segment for pipline processing
* augment: For adding or remoing rows. 
* cast: Holds the Caster, which cast the row value types to those specified in the destination schema. 
* last: The last segment before output processing. 
* store: Used internally for selecting a partition and writing rows to partitions. 