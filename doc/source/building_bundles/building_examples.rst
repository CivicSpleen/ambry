.. _building_examples:


Building Example Bundles
========================

There are a few public example bundles that you can build and analyze, which you can get by cloning the `example.com repository from Github <https://github.com/CivicKnowledge/example-bundles>`_. In this tutorial, we'll get the bundle sources, build them, and expore the results. 

.. code-block:: bash

    # Go to the source directory and clone the demo bundle. You can also run `ambry info` and cd to the
    # source directory. 
    
    $ source_dir=$(ambry info | grep Source | awk -F: '{print $2}') # Find your source directory from the config
    $ cd $source_dir
    
    # Clone the repo from Github
    
    $ git clone https://github.com/CivicKnowledge/example-bundles.git
    
    
A Simple Bundle
***************

All of the operations you can perform on the bundle are managed by the :command:`ambry` command, but it requires akward syntax to reference the bundle in the current directory, so we'll use the convenience command :command:`bambry`. :command:`bambry info` will give you some basic information about the bundle.

First, let's build the ``simple`` bundle. Change to the :file:`example-bundles/example.com/simple-builder` directory and run the :command:`bambry info` command to get basic information about the bundle. 

.. code-block:: bash

    $ cd example-bundles/example.com/simple-builder
    $ bambry info
    Title     : Simple Example Bundle
    Summary   : This is a short summary of the data bundle.
    VID       : dfY0NJqcai003
    VName     : example.com-simple-builder-0.1.3
    DB        : ... build/example.com/simple-builder-0.1.3.db
    Geo cov   : []
    Grain cov : []
    Time cov  :
    
This information shows the names and number of the bundle, and the path to where the bundle database will be created. 

To build the bundle, use the :command:`bambry build` command. Use the ``--clean`` flag to ensure a complete rebuild.: 

.. code-block:: bash

    $ bambry build --clean 
    INFO example.com-simple ---- Cleaning ---
    INFO example.com-simple Removing 
    ...
    INFO example.com-simple ---- Done Preparing ----
    INFO example.com-simple ---- Build ---
    INFO example.com-simple Finalizing partition: example.com-simple-example
    INFO example.com-simple ---- Done Building ---
    INFO example.com-simple Bundle DB at: sqlite:////...build/example.com/simple-0.1.3.db 
    
It will run for a few seconds before printing out the path to the bundle database. 

When it is complete, the bundle will have several partitions in the :file:`build` directory:

.. code-block:: bash

    build
    ├── example.com
    │   ├── simple-0.1.3
    │   │   └── example.db
    │   └── simple-0.1.3.db
    ├── meta
    │   └── documentation.html
    └── schema-revised.csv
        
The :file:`simple-0.1.3.db` is the bundle database, which holds the schema, names and numbers, configuration values, and references to the partitions. Each of the database files below the :file:`simple-0.1.3` is a partition database, such as :file:`example.db`. 

You can explore these databases with a Sqlite browser, or use the :command:`sqlite3` command line tool. For instance, to see what tables are in the bundle database, and list all of the partitions: 

.. code-block:: bash

    $ sqlite3 build/example.com/simple-0.1.3.db ".tables"
    codes       columns     datasets    partitions
    colstats    config      files       tables
    $
    $ sqlite3 -header build/example.com/simple-0.1.3.db "SELECT p_vname, p_cache_key FROM partitions"
    p_vname|p_cache_key
    example.com-simple-example-0.1.3|example.com/simple-0.1.3/example.db

This bundle has only one table, and one partition. Most bundles have multiple partitions, and some have several hundred. 

The Build Process Phases
************************

Building a bundle involves a series of stages:

- The optional **meta** phase, which is normally only run once to create the schema and extract variable and value codess from external documentation. 
- The **prepare** phase, to load the schema and create an empty bundle database.
- The **build** phase, which creates partitions and loads them with data. 
- The **install** phase, which installs the partitions into the library. 

Each of these phases has a pre, main and post sub-phases, although it is very rare for bundle developers to alter the pre and post sub phases. 

This example bundle does not have a meta phase, so we will discuss only the prepare and build phases. 

The **prepare** phase is run by executing the :command::`bambry prepare` command, but it is also run before the build phase if it hasn't been run first. In fact, running :command:`bambry build --clean` will actually run the clean, prepare then build phases, in that order. 

The major function of the prepare phase is to load the schema. The :file:`meta/schama.csv` is parsed, tables and columns are created in the bundle database. You can test this by running :command:`bambry prepare` and then display the tables or columns from the bundle database. 

.. code-block:: bash

    $ bambry prepare --clean #  --clean is required if bundle is already built
    INFO example.com-simple ---- Cleaning ---
    INFO example.com-simple Removing 
    ...
    INFO example.com-simple ---- Done Preparing ----
    $ sqlite3 build/example.com/simple-0.1.3.db "SELECT * from tables"
    tfY0NJscai01003|tfY0NJscai01|dfY0NJscai|dfY0NJscai003||1|example||A Pretty Good Table|||table|||{}
    
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

.. code-block:: bash

    $ bambry info -P -S
    Title     : Simple Example Bundle
    Summary   : This is a short summary of the data bundle.
    VID       : dfY0NJqcai003
    VName     : example.com-simple-builder-0.1.3
    DB        : .../build/example.com/simple-builder-0.1.3.db
    Geo cov   : []
    Grain cov : []
    Time cov  : 
    Created   : 2015-06-15T20:40:43.148545
    Prepared  : 2015-06-15T20:40:43.866714
    Built     : 2015-06-15T20:40:46.896717
    Build time: 1.34s
    Parts     : 1
    -----Partitions-----
              : example.com-simple-builder-example
    -----Partition example.com-simple-builder-example-0.1.3~pfY0NJqcai001003-----
    Stats     : 
    Col Name            :   Count    Uniq       Mean Sample Values                                                         
    int                 :   10000     101   5.03e+01 ▇▇▇▇▇▇▉▇▇▉                                                            
    float               :   10000   10000   4.98e+01 ▉▇▉▉▉▉▇▉▇▇                                                            
    id                  :   10000   10000                                                                                  
    uuid                :   10000   10000            93375c45-4d51-4149-b0b6-be497d70e
                                                     07a,0a2afeb4-b482-46b2-bae0-839312a2b6fa
                                                     f87a47d,74c22afb-5e66-49cb-9d61-4b426fd85591


The Info command, with the `-P` and `-S` options will return the statistics for all of the partitions, showing metrics to help determine if the build completed correctly. 

Final Steps: Installing
***********************

Once you have built a biundle, you can install it in the library with :command:`bambry install`:

.. code-block:: bash

    $ bambry install 
    INFO example.com-simple-builder ---- Install ---
    INFO example.com-simple-builder Install   example.com-simple-builder to  library sqlite:////.../data/library.db
    INFO example.com-simple-builder Installed .../example.com/simple-builder-0.1.3.db
    INFO example.com-simple-builder Install   example.com-simple-builder-example
    INFO example.com-simple-builder Installed .../example.com/simple-builder-0.1.3/example.db
    INFO example.com-simple-builder ---- Done Installing ---

The install process reports the file where the main database and partitions are copied. When it is complete, you can see the bundle in your library:

.. code-block:: bash

    $ ambry list example.com
    L      dfY0NJqcai003     example.com-simple-builder-0.1.3
    
And you can inspect it or open it: 

.. code-block:: bash

    $ ambry info -p dfY0NJqcai003
    D --- Dataset ---
    D Vid       : dfY0NJqcai003
    ...
    B Build time: 1.34s
    P --- Partition ---
    P Partition : pfY0NJqcai001003; example.com-simple-builder-example-0.1.3
    P Is Local  : True
    P Rel Path  : example.com/simple-builder-0.1.3/example.db
    P Abs Path  : /Users/eric/proj/virt/ambry-master/data/library/example.com/simple-builder-0.1.3/example.db
    ...
    
    $ ambry library open pfY0NJqcai001003
    sqlite> select max(int) from example; 
    100
    
Finally, you can push the library to the remote with :command:`ambry library push`, although this will only work after you've set up a S3 remote store. 

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
    :emphasize-lines: 14, 17, 26
    
    from  ambry.bundle import BuildBundle
 
    class Bundle(BuildBundle):
        ''' '''
 
        def __init__(self,directory=None):
        
            super(Bundle, self).__init__(directory)
 
        def build(self):
            import uuid
            import random

            p = self.partitions.new_partition(table="example")
            p.clean()
        
            with p.database.inserter() as ins:
                for i in range(10000):
                
                    row = dict(
                       uuid = str(uuid.uuid4()),
                       int = random.randint(0,100),
                       float = random.random()*100
                    )    

                    ins.insert(row)
                
            p.close()
                
            return True

This ``build`` method shows the basic pattern of most bundles, which involves:

1. Creating a partition ( line 14 )
2. Creating in an inserter for the partition ( line 17 )
3. Inserting records into the partition with the inserter ( line 26 ) 


Using a Loader Bundle
*********************

The most frequent source of data for a bundle is one or more CSV or Excel files. Because loading these files is so common, there is a set of classes to make loading them easier. Most of the time, loading one of the supported file types can be done with no python code, just some configuration. Change to the :file:`example-bundles/example.com/simple-loader` directory to see an example of a CSV file Loader. 

The :file:`bundle.py` for this bundle is a lot simpler. 

.. code-block:: python 

    from ambry.bundle.loader import CsvBundle

    class Bundle(CsvBundle):
        pass


Instead, the configuration is in the file :file:`sources.csv`. That, along with the :class:`CsvBundle` class is enough to build the bundle. Run :command:`bambry build` or  :command:`bambry build --clean` to see it build. 


Modifying Data in a Loader
**************************

Often, the data from a source file need to be altered before commiting it to a partition, such as by parsing date or adding columns, or other changes. You can add methods to the bundle class to make these changes. Change to the :file:`example-bundles/example.com/simple-loader-mod` directory and look at the :file:`bundle.py` file. 

.. code-block:: python 

    from ambry.bundle.loader import CsvBundle

    class Bundle(CsvBundle):

        @staticmethod
        def int_caster(v):
            """Remove 'NA' values from an int column"""
            if isinstance(v,int):
                return v

            if v.strip() == 'NA' :
                return -1
            else:
                return int(v)
       
        @staticmethod
        def real_caster(v):
            """Remove 'NA' values from a float column"""
            if v.strip() == 'NA' :
                return None
            else:
                return float(v)
            
        def build_modify_row(self, row_gen, p, source, row):
            """Make some random changes to the row to demonstrate build_modify_row"""
            import hashlib
        
            partition_name = str(p.identity.name)
            source_url = source.url
        
            if row['int'].strip() != 'NA' :
                row['int'] = int(row['int']) * 2
        
            row['uuid'] =  hashlib.md5(partition_name+source_url+row['id']).hexdigest()
    
In this bundle, most of the process is controlled but the :class:`CsvBundle` loader and the methods alter the values or rows as they are added to the partition. 

* :method:`int_caster` and :method:`real_caster` are attached to columns in :file:`schema.csv`, and are called on a cell in a row to cast or alter the value. 
* :method:`build_modify_row` is called on a whole row, so it can be used to make large changes to the row, such as looking up the value in one column to create another column. 

These are just the simplest bundles. For a more comprehensive tutorial for creating a more complex bundle, see :ref:`build a new bundle tutorial <creating>`.
