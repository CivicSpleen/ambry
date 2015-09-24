
Building the Example Bundles
============================

The ambry source distribution has a set of example bundles that are useful for 
learning how to use the ambry library. This tutorial will guide you through loading the example bundles into the library, building them, and exploring the results. 


Get the Source
**************


If you don't already have the ambry source distribution:

.. code-block:: bash

    $ git clone https://github.com/CivicKnowledge/ambry.git
    $ cd ambry 
    $ git checkout ambry1.0
    $ cd .. # So the next step doesn't use the ambry module in this dir
    
Import the Example Bundles
**************************

With the source available, import the bundle into the library, but use the '-d' option to detach the source. When the bundle source is later exported, it will be exported to the default source directory, not to the directory of the originally imported source. If you are a developer, this prevents the source tree from getting polluted with source files. 

.. code-block:: bash

    $ ambry import -d  ambry/test/bundles/
    Loading bundle: example.com-simple-0.1.3~d000simple003
    Loading bundle: example.com-segments-0.1.1~d0segments001
    Loading bundle: example.com-process-0.1.3~d00process003
    Loading bundle: example.com-generators-0.1.1~dgenerator001
    Loading bundle: example.com-dimension-0.1.1~ddimension001
    Loading bundle: example.com-complete-ref-0.1.3~d000000ref003
    Loading bundle: example.com-complete-load-0.1.3~d00000load003
    Loading bundle: example.com-complete-build-0.1.3~d0000build003
    Loading bundle: example.com-casters-0.1.3~d00casters003
    
Now, you can list the bundles:

.. code-block:: bash

    $ ambry list 
    vid            vid            vname                             state
    -------------  -------------  --------------------------------  -------
    d000simple003  d000simple003  example.com-simple-0.1.3          new
    d0segments001  d0segments001  example.com-segments-0.1.1        new
    d00process003  d00process003  example.com-process-0.1.3         new
    dgenerator001  dgenerator001  example.com-generators-0.1.1      new
    ddimension001  ddimension001  example.com-dimension-0.1.1       new
    d000000ref003  d000000ref003  example.com-complete-ref-0.1.3    new
    d00000load003  d00000load003  example.com-complete-load-0.1.3   new
    d0000build003  d0000build003  example.com-complete-build-0.1.3  new
    d00casters003  d00casters003  example.com-casters-0.1.3         new

Using the Bambry Command
************************

You can operate on these bundles with the :command:`bambry` program. There are a few ways to specify which bundle you want to work on:

* Use the ``-i`` option to specify the bundle vid explicity. 
* :command:`cd` to the bundle directory.  :command:`bambry` will find a bundle ref in a :file:`bundle.yaml` file in the current directory
* Set the `AMBRY_BUNDLE` env var.
* For most commands, supply the vid as an argument.
* Without any of the prior methods, :command:`bambry` will use the last bundle you worked on. 

Here are examples of all of the options: 

.. code-block:: bash

    $ cd <bundle_dir>; bambry info
    $ AMBRY_BUNDLE=d00process003 bambry info
    $ bambry -i d000simple003 info
    $ bambry info d000simple003
    $ bambry info

A common pattern is to use `-i` for the first operation, and after that, don't specify anything, and :command:`bambry` will continue to use the prior bundle. If you want to see what bundle will be used if one isn't specified, run :command:`bambry info -w` 
 
If the bundles were imported correctly, they should have some source or table records, which you can check with the :command:`bambry dump` command:

.. code-block:: bash

    $ bambry -i d000000ref003 dump -s 
    Dumping datasources for example.com-complete-ref-0.1.3~d000000ref003

    vid                  sequence_id  name    title    dest_table_name 
    -----------------  -------------  ------  -------  -----------------  
    S000000ref0001003              1  simple  simple   simple  
    
There are a lot of :command:`dump` subcommands available for exploring the bundle. 
    
Build the Bundles
*****************

If that looks good, you can build the bundles. Because these bundles were created for testing, the bundles are in various states of development, so you should go through all three phases with them. These phases are: 

* *Ingest* to load the source files. 
* *Schema* to create tables from the source files. 
* *Build* to actually build the bundle. 

.. code-block:: bash

    $ bambry -i d000simple003 ingest
    $ bambry schema
    $ bambry build

The ``ingest`` and ``schema`` phases  update the bundle source files, so the next time the bundle is checked out and build, you'd only need to run the ``build`` phase. 

After the build is complete, the bundle list should show that the ``simple`` bundle has been built:

.. code-block:: bash

    $ ambry list 
    vid            vid            vname                             state
    -------------  -------------  --------------------------------  -------
    d000simple003  d000simple003  example.com-simple-0.1.3          build_done
    d0segments001  d0segments001  example.com-segments-0.1.1        new
    d00process003  d00process003  example.com-process-0.1.3         new
    dgenerator001  dgenerator001  example.com-generators-0.1.1      new
    ddimension001  ddimension001  example.com-dimension-0.1.1       new
    d000000ref003  d000000ref003  example.com-complete-ref-0.1.3    new
    d00000load003  d00000load003  example.com-complete-load-0.1.3   new
    d0000build003  d0000build003  example.com-complete-build-0.1.3  new
    d00casters003  d00casters003  example.com-casters-0.1.3         new

Check your Work
***************

You can review the build with the :command:`bambry info` command. With the ``-P`` option, it will list the data partitions, and with the ``-S`` option, it will display statistics:

.. code-block:: bash

    $ bambry info -P
    Title    Simple Example Bundle
    Summary  This is a short summary of the data bundle
    VID    d000simple003             Build State  build_done
    VName  example.com-simple-0.1.3
    Build  FS  <OSFS: /Users/eric/proj/virt/ambry10/library/build/example.com/simple-0.1.3>
    Source FS  <OSFS: /Users/eric/proj/virt/ambry10/demo-source/example.com/simple-0.1.3>

    Partitions
    Vid               Name                             Table    Time    Space    Grain
    ----------------  -------------------------------  -------  ------  -------  -------
    p000simple002003  example.com-simple-simple-0.1.3  simple

Look in the directories specified by the ``Build FS`` URL, and you will find all of the build files, including the ingested sources, output files and pipeline output logs that describe the main stages of the build. 

If you want to get the data in your bundle extracted to a CSV file, run: 

.. code-block:: bash

    $ bambry extract
    Using bundle ref d000simple003, referenced from history
    INFO example.com-simple Extracting: example.com-simple-simple 
    INFO example.com-simple Extracted to: /Users/eric/proj/virt/ambry10/library/build/example.com/simple-0.1.3/extract
    (ambry10)[eric@gala doc]$ open /Users/eric/proj/virt/ambry10/library/build/example.com/simple-0.1.3/extract/example.com-simple-simple.csv 
    
The files in the build directory with a ``.mpr`` extension is a proprietary format. You can view these files with the :command:`ampr` command: 

.. code-block:: bash
    $ ampr -H /Users/eric/proj/virt/ambry10/library/build/example.com/simple-0.1.3/example.com/simple-0.1.3/source/simple.mpr 
    MPR File    : /Users/eric/proj/virt/ambry10/library/build/example.com/simple-0.1.3/example.com/simple-0.1.3/source/simple.mpr
    Created     : 2015-09-22 16:12:42.336765
    version     : 1
    rows        : 10001
    cols        : 4
    header_rows : [0]
    data_row    : 1
    end_row     : 10001
    URL         : http://public.source.civicknowledge.com/example.com/sources/simple-example.csv

    HEAD
      #    id  uuid                                    int    float
    ---  ----  ------------------------------------  -----  -------
      0     1  eb385c36-9298-4427-8925-fe09294dbd5f     30  99.7347
      1     2  fbe2ba34-b130-49b7-bd84-3dc6efb63266     79  18.7601
      2     3  b63c1b4c-0d48-43ae-9f1d-83b0291462b5     21  34.2059
      3     4  bcf29f19-79f3-427d-b068-898e21bdc933     52  85.1948
      4     5  f02d53a3-6bbc-4095-a889-c4dde0ccf577    100  20.3416
      5     6  2ba85adb-4f0b-428b-b947-f4227b5b2979     16  86.6151
      6     7  56f8800e-cad7-4a59-bde4-70cba84eef50     60   4.8741
      7     8  58c35b80-be66-40dc-b1a4-f43a208e0acb     89  10.9109
      8     9  566f968f-2f2a-4b59-9d3f-18261722e6f2     69  97.6747
      9    10  d1a7fd5d-4ebb-4ef1-b9b0-d7696157aa45     62  33.657
    

Bundle Source
**************

One thing we haven't seen yet is the source. When the sources were imported from the ambry source directory, we used the ``-d`` option so changes to the source files would not be written back into the ambry source directory. The imported bundle will store sources in the default location, which is specified in the ``Source FS`` url. But, that directory is empty now, because the source files have not been exported to it. 

To export the bundle source files, run :command:`bambry export`. Then visit the source directory to see some of the files that were involved in making the bundle. 



Other Tips
**********

Make changing directories a bit easier by installing the helper functions. You can put this into your ``.bashrc`` so its always there: 

.. code-block:: bash

    $ source $(which ambry-aliases.sh )
    
    

