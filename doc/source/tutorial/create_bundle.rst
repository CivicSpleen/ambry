Create a New Bundle
===================

Creating a new bundle involves two phases, the meta phase and the build phase. The meta phase is usually only run once, while the bundle is being configured. The Build phase actually creates the bundle partitions, and can be run by others, on other machines, after the bundle is configured. 

In this tutorial, we will create a new bundle, using the `USDA Farmer's Market list
<http://public.source.civicknowledge.com/usda.gov/farmers-markets.csv>`_. 

In the meta phase, you will create the new bundle and configure the files. The steps in this phase are usually: 

- Create the new bundle with :command:`bambry new`, then export the files to a directory
- Edit the :file:`sources.csv` to refer to the input source files.
- Ingest the files with :command:`bambry ingest`, edit :file:`sources.csv` until ingestion runs smoothly. 
- From the ingested files, create the source schema with :command:`bambry schema -s`
- Possibly edit the schemas to alter column names 
- Create the destination schemas with :command:`bambry schema -d`
- Edit the destination schemas so the source files build properly

After configuring the bundle, you can build it to generate the data partitions. 

Create a new bundle
*******************

Use the :command:`bambry new` command to create a new bundle, then export the bundle files to your working directory.  To export a new bundle, the :command:`bambry export` command works like :command:`bambry sync -o`, but can also create directories and set the bundles source directory. 

.. seealso::
    :ref:`Overview of files and objects <file_locations>`
        For an explaination about why you have to sync files out.

.. code-block:: bash

    $ bambry new -s usda.gov -d farmers_markets
    usda.gov-farmers_markets-0.0.1~dQH4kt5xlf001
    
The command will create a new bundle in your library, and print out the fully-qualified name, which includes the version number and vid. Run :command:`ambry list` to verify that the bundle was created. 

.. code-block:: bash

    $ ambry list farmers
    vid            vname                           dstate    bstate    about.title
    -------------  ------------------------------  --------  --------  -------------
    dQH4kt5xlf001  usda.gov-farmers_markets-0.0.1  new       new

You can add any word to :command:`ambry list` and it will work like :command:`grep`, returning only the bundles that have that word in their names.

If your current directory is not in another bundle directory, the command will also have set the working bundle. Run :command:`bambry info -w` to see what bundle this is:

.. code-block:: bash

    $ bambry info -w 
    Will use bundle ref dQH4kt5xlf001, usda.gov-farmers_markets-0.0.1, referenced from history
    
When the working bundle is set, you can run :command:`bambrycd` to cd to the bundle's build directory, or if the working bundle is not set, use a bundle reference, such as the vid. In our case, that's :command:`bambrycd dQH4kt5xlf001`

.. code-block:: bash

    $ bambrycd dQH4kt5xlf001

.. note::

    When you specify a reference to a bundle, you can use one of several differnt forms, including the id, vid, name or vname. For :command:`bambrycd`, these are all equivalent: 

    .. code-block:: bash

        $ bambcycd dQH4kt5xlf001
        $ bambrycd dQH4kt5xlf
        $ bambrycd usda.gov-farmers_markets-0.0.1
        $ bambrycd usda.gov-farmers_markets

    Unversioned references are resolved to the latest version, so in this case, `dQH4kt5xlf` will resolve to `dQH4kt5xlf001`
    
If you list the contents of this directory, you'll notice that it is empty. That's because we have not synced the files out. There are two ways to sync out. When you've created a new bundle, you'll want to use :command:`bambry export`:

.. code-block:: bash

    $ bambry export 
    Using bundle ref dQH4kt5xlf001, referenced from history
    INFO dQH4kt5xlf001 ---- Sync Out ----
    INFO dQH4kt5xlf001 Sync: sources.csv
    INFO dQH4kt5xlf001 Sync: bundle.py
    INFO dQH4kt5xlf001 Sync: source_schema.csv
    INFO dQH4kt5xlf001 Sync: lib.py
    INFO dQH4kt5xlf001 Sync: test.py
    INFO dQH4kt5xlf001 Sync: bundle.yaml
    INFO dQH4kt5xlf001 Sync: schema.csv
    
The unadorned :command:`bambry export` works well when you are writing the bundle into the default soruce directly, but if you have a specific location to export to, such as a git repository, you will want to specify a specific directory and maybe add the `-a` option. The `-a` option will use the standard soruce naming scheme. 

So, :command:`bambry export /tmp/foo` would write the bundle build files to :file::`/tmp/foo`, while :command:`bambry export -a /tmp/foo` would write to :file:`/tmp/foo/usda.gov/farmers_markets`. 

.. caution::

    Running :command:`bambry export` with a destination directory will set your bundle's build directory, so future file operations will go to that directory. You can check this with :command:`bambry info -s`:
    
    .. code-block:: bash
    
        $ bambry info -s
        /tmp/foo/usda.gov/farmers_markets/
        
    To set the source directory back, you can export again, or cd to the directory you want and run :command:`bambry set -S`
  
You should now have all of the default files in your bundle's source directory.

Adding and Ingesting Sources
****************************

Open the :file:`sources.csv` file in a spreadsheet editor and add  a new row with these values: 

- name: farmers_markets
- ref: http://public.source.civicknowledge.com/usda.gov/farmers-markets.csv

After you save the file, you can load it into the library with :command:`bambry sync -i`, then veryify that it was loaded by dumpoing the sources with :command:`bambry dump -s`:

.. code-block:: bash

    $ bambry sync -i 
    Using bundle ref dQH4kt5xlf001, referenced from directory
    Sync in
    INFO dQH4kt5xlf001 ---- Sync In ----
    INFO dQH4kt5xlf001 Sync: sources.csv
    INFO dQH4kt5xlf001 Sync: source_schema.csv
    INFO dQH4kt5xlf001 Sync: schema.csv
    $ bambry dump -s 
    Dumping datasources for usda.gov-farmers_markets-0.0.1~dQH4kt5xlf001

    vid                    ref               state
    -----------------  ... ------------ ...  -------
    SQH4kt5xlf0001001      http://publi ...  synced
    
Now you can ingest the file. Ingesting loads the source file into an MPR file, a custom data file format that allows for quick access for additional analysis, including inferring data types, categorizing rows, and computing statistics.  The :command:`bambry ingest` command, without additional arguments, will ingest all of the source files, of which we have only one. However, with many files, you'll want to ingest them seperately. We'll use the :option:`-s` option to specify a single source file. 

.. code-block:: bash  

     $ bambry ingest -s farmers_markets
     Using bundle ref dQH4kt5xlf001, referenced from directory
     INFO dQH4kt5xlf001 ---- Ingesting ----
     INFO dQH4kt5xlf001 ingest:1 > Ingesting SP processed 1 of 1 source
     INFO dQH4kt5xlf001 ingest:1 + SQH4kt5xlf0001001 Ingesting source #1, farmers_markets
     INFO dQH4kt5xlf001 ingest:1 . SQH4kt5xlf0001001 Source farmers_markets already ingested, skipping
     INFO dQH4kt5xlf001 ingest:1 < Successful context exit
     INFO dQH4kt5xlf001 Ingested 1 sources
     
To verify that the file was loaded, run :command:`bambry dump -i` to see the ingested files. Then, you can run :command:`bambry view` to see the file. 

.. code-block:: bash  

    $ bambry view farmers_markets
    Using bundle ref dQH4kt5xlf001, referenced from directory
    MPR File    : .../build/usda.gov/farmers_markets/ingest/farmers_markets.mpr
    Created     : 2016-01-18 12:40:08.159477
    version     : 1
    rows        : 8501
    cols        : 59
    header_rows : [0]
    data_row    : 1
    end_row     : 8501
    URL         : http://public.source.civicknowledge.com/usda.gov/farmers-markets.csv

Review this information to see if Ambry loaded the file as you expect. In particular, check that `rows` and `cols` seem like the right number of rows and columns in the file. Then look at `header_rows` and `data_row.` THe `header_rows` value is a list of the rows that contain the names of the columns. It should be just the first row for CSV files, but for excel files, there may be multiple rows that have the column headers. The `data_row` value is the row number of the first data row. 

With additional options, the :command:`bambry view` command can show you other information about the file:

- :option:`-H` prints the first 10 rows and leftmost 10 columns, to see if the structure of the file looks corrrect. 
- :option:`-s` prints the schema of the file, including the column names and a guess at the data type of the column. 
- :option:`-t` shows the couts of each datatype for each column, so you can see how the Type Intuiter made its guesses about the column dat types. 

View the file with some of the other options to check if it looks right. 

Ingesting the file will also update the source records, which you can export to the :file:`source.csv` file. This will add to values for `start_line`, which is important, and `end_line`, which is usually just informative.  You can verify that the source now has the values from the ingestion process by running :command:`bambry dump -s` and looking for the ``start_line`` and ``end_line`` values. 

Then sync out to get the updates to the sources into the :file:`source.csv` file. 

.. code-block:: bash 
    
    $ bambry sync -o

.. caution::

    If you don't sync out the updates to sources, or manually set the start_line in the :file:`source.csv` file, you may get an error in the build process when Ambry tries to load the first ( 0'th ) row as a data row. 


Additional Source Configuration
-------------------------------

The :file:`sources.csv` file has a lot of specification values to configure a source, which are (will, TBD) covered in another documentation section. But to briefly note, some of the things you can specify are: 

- Encoding, usually `latin1` or `utf-8`, but all common character encodings are supported. 
- Different file formats, including Excel, fixed width, tab delimited and Shapefiles.
- Non standard start lines, so header and comment rows in Excel files can be ignored

Creating Schemas
****************

After ingesting the source files, you can create the source and destination schemas. The source schema defines the column names and data types for each source file. It's basically what you see when you run :command:`bambry view -s` on an ingested source. The destination schema is also a declaration of column names and types, but it is for the output, and is attached to the partitions. 

Creating a source schema is easy: run :command:`bambry schema` to build all of the soruce schemas, or :command:`bambry schema -s <sourcename>` to build for a specific source. After building the source schema, you can check it was created with :command:`bambry dump -T` and write it back to the :file:`source_schema.csv` file with :command:`bambry sync -o`

.. code-block:: bash

    $ bambry schema -s farmers_markets
    Using bundle ref dQH4kt5xlf001, referenced from directory
    INFO dQH4kt5xlf001 Creating source schema for: farmers_markets; 59 columns
    Created source schema
    $ bambry dump -T
    Dumping sourcetables for usda.gov-farmers_markets-0.0.1~dQH4kt5xlf001

    vid                    table              position  source_header    ...
    ---------------------  ---------------  ----------  ---------------  ...
    CQH4kt5xlf00010001001  farmers_markets           1  fmid             ...
    CQH4kt5xlf00010002001  farmers_markets           2  marketname       ...
    CQH4kt5xlf00010003001  farmers_markets           3  website          ...
    CQH4kt5xlf00010004001  farmers_markets           4  facebook         ...
    $ bambry sync -o 
    Using bundle ref dQH4kt5xlf001, referenced from directory
    Sync out
    INFO dQH4kt5xlf001 ---- Sync Out ----
    INFO dQH4kt5xlf001 Sync: source_schema.csv
    
.. hint:: 

    If your bundle database state gets corrupt or diverged from what is defined in the build files, you can clean out the bundle with :command:`bambry clean`, then load the files back in with :command:`bambry sync -i`, or do both in one command with :command:`bambry clean -y`

After creating the source schema, you can create the destination schema, which is the description of the table that will be included in the output partitions. TO create a destination schema, run :command:`bambry schema -d` and then sync out the :file:`schema.csv` with the :command:`bambry sync -o` command. You can verify that the schema was created with :command:`bambry dump -t` to view the table, and :command:`bambry dump -C` to view the columns. 


.. code-block:: bash

    $ bambry schema -d 
    Using bundle ref dQH4kt5xlf001, referenced from directory
    INFO dQH4kt5xlf001 ---- Schema ----
    INFO dQH4kt5xlf001 Populated destination table 'farmers_markets' from source table 'farmers_markets' with 61 columns
    Created destination schema
    $ bambry dump -t
    Dumping tables for usda.gov-farmers_markets-0.0.1~dQH4kt5xlf001

    vid                sequence_id  name               c_sequence_id
    ---------------  -------------  ---------------  ---------------
    tQH4kt5xlf02001              2  farmers_markets                1
    $ bambry dump -C | wc
          67     332    5110
    $ bambry sync -o 

Building the Bundle
*******************

Build the bundle with: :command:`bambry build`. It should build cleanly:

 
.. code-block:: bash

    $ bambry build 
    Using bundle ref dQH4kt5xlf001, referenced from directory
    INFO dQH4kt5xlf001 ==== Building ====
    INFO dQH4kt5xlf001 build > 
    INFO dQH4kt5xlf001 Processing 1 sources, stage 0 ; first 10: [u'farmers_markets']
    INFO dQH4kt5xlf001 build + SQH4kt5xlf0001001 Running source farmers_markets
    INFO dQH4kt5xlf001 build . SQH4kt5xlf0001001 Running pipeline farmers_markets: rate: 1059.34 processed 6000 rows
    INFO dQH4kt5xlf001 build . SQH4kt5xlf0001001 Finished building source processed 6000 rows
    INFO dQH4kt5xlf001 build . SQH4kt5xlf0001001 Finalizing segment partition
    INFO dQH4kt5xlf001 build . SQH4kt5xlf0001001 Finalizing segment partition usda.gov-farmers_markets-farmers_markets-1
    INFO dQH4kt5xlf001 build . SQH4kt5xlf0001001 Finished processing source
    INFO dQH4kt5xlf001 coalesce > Coalescing partition segments
    INFO dQH4kt5xlf001 coalesce + Colescing partition usda.gov-farmers_markets-farmers_markets processed 1 partitions
    INFO dQH4kt5xlf001 coalesce . Coalescing single partition usda.gov-farmers_markets-farmers_markets-1  processed 1 partitions
    INFO dQH4kt5xlf001 coalesce . Running stats  processed 1 partitions
    INFO dQH4kt5xlf001 coalesce < Successful context exit
    INFO dQH4kt5xlf001 build < Successful context exit
    INFO dQH4kt5xlf001 ==== Done Building ====
    
    
    
    

Improving the Output
*********************

Now it is time to build the bundle. Run the :command:`bambry build` command. Unfortunately, this bundle has some problems. You should see the start of the build process, then a detailed "Pipeline Exception"

.. code-block:: bash

    $ bambry build
    Using bundle ref dQH4kt5xlf001, referenced from directory
    INFO dQH4kt5xlf001 ==== Building ====
    INFO dQH4kt5xlf001 build > 
    INFO dQH4kt5xlf001 Processing 1 sources, stage 0 ; first 10: [u'farmers_markets']
    INFO dQH4kt5xlf001 build + SQH4kt5xlf0001001 Running source farmers_markets
    INFO dQH4kt5xlf001 build + 
    ======================================
    Pipeline Exception: ambry.etl.pipeline.PipelineError
    Message:         Failed to cast column in table farmers_markets: Failed to cast column 'fmid' value='FMID' to '<type 'int'>': Failed to cast to integer
    Pipeline:        build
    Pipe:            ambry.etl.pipeline.CastColumns
    Source:          farmers_markets, SQH4kt5xlf0001001
    Segment Headers: [u'id', u'fmid', u'marketname', u'website', u'facebook', u'twitter', u'youtube', u'othermedia', u'street', u'city', u'county', u'state', u'zip', u'zip_codes', u'season1date', u'season1time', u'season2date', u'season2date_codes', u'season2time', u'season3date', u'season3time', u'season4date', u'season4time', u'x', u'y', u'location', u'credit', u'wic', u'wiccash', u'sfmnp', u'snap', u'organic', u'bakedgoods', u'cheese', u'crafts', u'flowers', u'eggs', u'seafood', u'herbs', u'vegetables', u'honey', u'jams', u'maple', u'meat', u'nursery', u'nuts', u'plants', u'poultry', u'prepared', u'soap', u'trees', u'wine', u'coffee', u'beans', u'fruits', u'grains', u'juices', u'mushrooms', u'petfood', u'tofu', u'wildharvested', u'updatetime']

    -------------------------------------

    Pipeline:
    Pipeline build
    source: ambry.etl.pipeline.SourceFileSourcePipe; <class 'ambry.orm.source.DataSource'> public.source.civicknowledge.com/usda.gov/farmers-markets.csv
    source_map: ambry.etl.pipeline.MapSourceHeaders: map = {} 
    cast: ambry.etl.pipeline.CastColumns2 pipelines

    select_partition: ambry.etl.pipeline.SelectPartition selector = default
    write: ambry.etl.pipeline.WriteToPartition

    final: []

    INFO dQH4kt5xlf001 build < Failed in context with exception
    CRITICAL: Pipeline error: ambry.etl.pipeline.PipelineError; Failed to cast column in table farmers_markets: Failed to cast column 'fmid' value='FMID' to '<type 'int'>': Failed to cast to integer
    
This sort of error is, unfortunately, very common. It is due to a faliure of one of the `CastColumns` pipe to cast a string value in the `fmid` to the declared types for that column, an integer.  It's time to open up the :file:`schema.csv` file in a spreadsheet editor and fix the problem. 

When you open the file, most of it will seem sensible, by there are a few odd bits: 


- The datatype for the `zip` column is `types.IntOrCode`
- The next column, `zip_codes` has a `transform` value. 
- A similar situation exists for the `season2date` column. 

The `transform` column is a transformation to apply to a value as it is loaded into the partition. The transformation has it's own flow that is a lot like the pipeline, but for columns instead of entire rows. These transformation are handled by the CastColumns pipe and are run by a generate python file, which is stored in the bundle build directory. You can view this code at: :file:`$(bambry info -b)/code/casters/farmers_markets.py`. 

WHen we generated the source and destination schemas for the `farmers_market` file, Ambry notices that the `zip` and `season2date` columns are mostly one type, but have some strings too. So, while the other columns have a simple datatype, those two have an `OrCode` type. These are special data types that will try to parse a value to particular type, and if the parsing fails, will store the value as a string. This value can be retrieved later, in the `code` column. 

So, most of the time, `zip` is an integer. When it is not, the `zip` column will hold a NULL, but the `code` property will be set. Then, the transform for the `zip_code` column will pull out that code. The pipe character '|' seperates stages in the transform, with two of them meaning that the code is extracted after the first round of transforms has been run. The code value is set on the first stage, then it can be retrieved in the second round. 

This transform system allows for very sophisticated transformation of data, but can be very complicated, so lets simplify this one a bit. We'll do three things to this schema: 

# Fix the casting error with the `fmid` column. 
# Simplify the transform with the `zip` and `season2date` columns. 

Examining the file
------------------

To make our analysis easier, let's dump the ingested file to see what the problems with the columns are. We'll need to re-ingest it first, then extract it to a CSV file. 

    
.. code-block:: bash

    $ bambry ingest
    $ bambry view farmers_markets -c farmers_markets.csv
    
Now you can open :file:`farmers_markets.csv`















    