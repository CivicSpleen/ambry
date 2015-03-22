.. _building_examples:


Building Example Bundles
========================

The :file:`test` directory has a set up example directories that are a good place to start for understanding how to build your own bundles. 

First, :command:`cd test/bundles/example.com` to see a list of the bundle directories that are available. We'll start with the `simple` directory.

All of the operations you can perform on the bundle are managed by the :command:`ambry` command, but it requires akward syntax to reference the bundle in the current directory, so we'll use the convenience command :command:`bambry`. :command:`bambry info` will give you some basic information about the bundle: 

.. code-block:: bash

    $ bambry info
    INFO example.com-simple ----Info: 
    INFO example.com-simple VID  : dfY0NJscai003
    INFO example.com-simple Name : example.com-simple
    INFO example.com-simple VName: example.com-simple-0.1.3
    INFO example.com-simple DB   : .../test/bundles/example.com/simple/build/example.com/simple-0.1.3.db
    
This information shows the names and number of the bundle, and the path to where the bundle database will be created. 

To build the bundle, use the :command:`bambry build` command. Use the `--clean` flag to ensure a complete rebuild.: 

.. code-block:: bash

    $ bambry build --clean 
    INFO example.com-simple ---- Cleaning ---
    INFO example.com-simple Removing /Users/eric/proj/ambry/test/bundles/example.com/simple/build/example.com/simple-0.1.3
    ...
    INFO example.com-simple Finalizing partition: example.com-simple-links
    INFO example.com-simple ---- Done Building ---
    INFO example.com-simple Bundle DB at: sqlite:////Users/eric/proj/ambry/test/bundles/example.com/simple/build/example.com/simple-0.1.3.db
    
It will run for a few seconds before printing out the path to the bundle database. 

When it is complete, the bundle will have several partitions in the :file:`build` directory:

.. code-block:: bash

    $ tree build
    build
    └── example.com
        ├── simple-0.1.3
        │   ├── example.db
        │   ├── example2.db
        │   ├── example3.db
        │   └── links.db
        ├── simple-0.1.3.db
        └── simple-0.1.3.log
        
The :file:`simple-0.1.3.db` is the bundle database, which holds the schema, names and numbers, configuration values, and references to the partitions. Each of the database files below the :file:`simple-0.1.3` is a partition database. 

You can explore these databases with a Sqlite browser, or use the :command:`sqlite` command line tool. For instance, to see what tables are in the bundle database, and list all of the partitions: 

.. code-block:: bash

    $ sqlite3 build/example.com/simple-0.1.3.db ".tables"
    codes       columns     datasets    partitions
    colstats    config      files       tables
    $
    $ sqlite3 -header build/example.com/simple-0.1.3.db "SELECT p_vname, p_cache_key FROM partitions"
    p_vname|p_cache_key
    example.com-simple-example-0.1.3|example.com/simple-0.1.3/example.db
    example.com-simple-example2-0.1.3|example.com/simple-0.1.3/example2.db
    example.com-simple-example3-0.1.3|example.com/simple-0.1.3/example3.db
    example.com-simple-links-0.1.3|example.com/simple-0.1.3/links.db

The Build Process Phases
************************

Building a bundle involves a series of stages:

# The optional **meta** phase, which is normally only run once to create the schema and extract variable and value codess from external documentation. 
# The **prepare** phase, to load the schema and create an empty bundle database.
# The **build** phase, which creates partitions and loads them with data. 
# The **install** phase, which installs the partitions into the library. 

Each of these phases has a pre, main and post sub-phases, although it is very rare for bundle developers to alter the pre and post sub phases. 

This example bundle does not have a meta phase, so we will discuss only the prepare and build phases. 

The **prepare* phase is run by executing the :command::`bambry prepare` command, but it is also run before the build phase if it hasn't been run first. In fact, running :command:`bambry build --clean` will actually run the clean, prepare then build phases, in that order. 

The major function of the prepare phase is to load the schema. The :file:`meta/schama.csv` is parsed, tables and columns are created in the bundle database. You can test this by running :command:`bambry prepare` and then display the tables or columns from the bundle database. 

.. code-block:: bash

    $ bambry prepare
    INFO example.com-simple ---- Pre-Prepare ----
    INFO example.com-simple Bundle has already been prepared
    INFO example.com-simple ---- Skipping prepare ----
    $ sqlite3 build/example.com/simple-0.1.3.db "SELECT * from tables"
    tfY0NJscai01003|tfY0NJscai01|dfY0NJscai|dfY0NJscai003|1|example||A Pretty Good Table|||table|||{}
    tfY0NJscai02003|tfY0NJscai02|dfY0NJscai|dfY0NJscai003|2|example2||Another Table|||table|||{}
    tfY0NJscai03003|tfY0NJscai03|dfY0NJscai|dfY0NJscai003|3|example3||Another Table|||table|||{}
    tfY0NJscai04003|tfY0NJscai04|dfY0NJscai|dfY0NJscai003|4|links||Links to Other tables|||table|||{}
    tfY0NJscai05003|tfY0NJscai05|dfY0NJscai|dfY0NJscai003|5|example4|||||table|||{}
    
After running the *prepare* phase, the *build* phase can be run. As shown in the earlier bash example the build phase will create partition objects in the bundle database, and create parittion databases in the build directory. These partition databases collectively hold all of the data in the bund. 

.. code-block:: bash

     $ bambry build
     INFO example.com-simple ---- Build ---
     INFO example.com-simple Finalizing partition: example.com-simple-example
     INFO example.com-simple Finalizing partition: example.com-simple-example2
     INFO example.com-simple Finalizing partition: example.com-simple-example3
     INFO example.com-simple Finalizing partition: example.com-simple-links
     INFO example.com-simple ---- Done Building ---
     INFO example.com-simple Bundle DB at: sqlite:////Users/eric/proj/ambry/test/bundles/example.com/simple/build/example.com/simple-0.1.3.db
    
In the post-build sub-phase, the partitions are finalized, which closes the partition database files and computed statistics for the partition's main table. The statistics describe the mean, deviation, count of value, number of unique values, and other useful information about each column in each table of each partition. Run :command:`bambry info -S` to see the bundle info and include the statistics. 



Bundle Configuration
********************

Let's explore the structure of the bundle a bit. There are four files that are most important in defining the operation of a bundle: 

- :file:`bundle.py` The main Python program
- :file:`bundle.yaml` The main configuration
- :file:`meta/build.yaml` The build configuration
- :file:`meta/schema.csv` Definitions of tables and columns

The core configuration file is :file:`bundle.yaml`. Here are the most important parts  of that file. 

.. code-block:: yaml

    about:
        summary: This is a short summary of the data bundle.
        title: Simple Example Bundle
    identity:
        bspace: null
        btime: null
        dataset: simple
        id: dfY0NJscai
        revision: 3
        source: example.com
        subset: null
        type: null
        variation: null
        version: 0.1.3
    names:
        fqname: example.com-simple-0.1.3~dfY0NJscai003
        name: example.com-simple
        vid: dfY0NJscai003
        vname: example.com-simple-0.1.3
        
The :file:`bundle.yaml` file defines the names, numbers, revision and titles for the bundle. The ``identiy`` and ``names`` sections are set when you creat the bundle, while the ``about`` section is edited manually.  You'll usually only have to edit the ``about``.
        
The  :file:`meta/build.yaml` holds configuration related to building the bundle, such as dependencies on other bundles and source URLs for files to download. 

The :file:`meta/schema.csv` file defines the tables and columns. There is one row in the file for each column, and the set of columns labeled with the same table name form the table. 

The :file:`bundle.py` file holds the code for building the bundle. For this bundle, there is only one method, ``build``, but other bundles may have methods for other build phases. 

.. code-block:: python 
    :linenos:
    :emphasize-lines: 13,16,27
    
    from  ambry.bundle import BuildBundle
 
    class Bundle(BuildBundle):

        def build(self):
            import uuid
            import random
            
            # For each of these tables, create a new partition and
            # insert 10,000 records of random data. 
            for table in ('example', 'example2','example3'):
            
                p = self.partitions.new_partition(table=table)
                p.clean()
            
                with p.database.inserter() as ins:
                    for i in range(10000):
                        row = dict()
            
                        row['uuid'] = str(uuid.uuid4())
                        row['int'] = random.randint(0,100)
                        row['float'] = random.random()*100
                        row['year'] = random.randint(0,100)
                        row['hu100'] = random.randint(0,100)
                        row['pop100'] = random.randint(0,100)
                
                        ins.insert(row)

            # This table is composed of foreign key links to 
            # two of the tables created above. 
            p = self.partitions.new_partition(table='links')
            p.clean()
        
            with p.database.inserter() as ins:
                for i in range(10000):
                    row = dict(example2_id = i, example3_id = i)
                
                    ins.insert(row)
            

            return True

This ``build`` method shows the basic pattern of most bundles, which involves:

1. Creating a partition ( line 13 )
2. Creating in an inserter for the partition ( line 16 )
3. Inserting records into the partition with the inserter ( line 27 ) 





