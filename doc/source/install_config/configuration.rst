.. _configuration:

Configuration
=============

Ambry uses two main configuration files, which can exist in a few different places. 

The first file is the the application configuration that specifies nearly everything. The configuration file will be placed in diffrent places depending on the environment in which the command is invoked:

* The path specified by the ``AMBRY_CONFIG`` environmental variable
* For root users, the configuration file is written to :file:`/etc/ambry.yaml`
* For non root users, :file:`$HOME/.ambry.yaml`
* In a python virtualenv, :file:`$VIRTUAL_ENV/.ambry.yaml`

The root directory for the ambry library also changes depending on the environment, although this can be changed in the coniguration file: 

* Root user: :file:`/var/ambry`
* Non root user: :file:`$HOME/ambry`
* Virtualenv: :file:`$VIRTUAL_ENV/data`

( To set the root directory, alter the `filesystem.root` value )

The second file holds account credentials. It is a user file, and is at ``$HOME/.ambry-accounts.yaml``

.. _configuration_generation:

Generating a Config File
************************

  
After installing Ambry and its dependencies, you may need to install a configuration file. Run :command:`ambry info` to see if a configuration file already exists. Install the default configuration file with:

.. code-block:: bash

    $ ambry config install 


 
Then, check that it worked with:
   
.. code-block:: bash
    
    $ ambry info 
    Version:  0.3.420
    Root dir: /home/eric/ambry
    Source :  /home/eric/ambry/source
    Configs:  ['/home/eric/.ambry.yaml', '/home/eric/.ambry-accounts.yaml']

    $ ambry library info 
    Library Info
    Name:     default
    Database: sqlite:////home/eric/ambry/library.db
    Cache:    FsCache: dir=/home/eric/ambry/library upstream=(None)
    Remotes:  FsCompressionCache: upstream=(HttpCache: url=http://s3.sandiegodata.org/library/)


The Main Configuration File
***************************

After installation, the main configuration file will be installed to a directory as described above. Run :command:`ambry info` to discover where it is. 

.. code-block:: yaml

    library:
        default:
            filesystem: '{root}/library'
            database: 'sqlite://{root}/library.db'
        remotes:
            public: 'http://public.library.civicknowledge.com'
            system: 'http://system.library.civicknowledge.com'

    filesystem:
        root: /var/ambry
        downloads: '{root}/cache/downloads'
        extracts: '{root}/cache/extracts'
        documentation: '{root}/cache/documentation'
        python: '{root}/cache/python'
        source: '{root}/source'
        warehouses: '{root}/warehouses'

    services:
        numbers:
            host: numbers.ambry.io
            key: fe78d179-8e61-4cc5-ba7b-263d8d3602b9
            port: 80


The file is composed of sections, each with a top level dictionary key. The keys are: 

* ``library``: Defines one or more library locations. Most files will only have 'default'.
* ``filesystem``: Defines directories for various purposes, execpt for the ``root`` key, which is substituted into other paths. 
* ``services``: Defines connection information for remote services. 
  
  
Library Section
---------------

The Library section declares the database, fielsystem and remote for your library. 

* ``database``: A connection URL for the library database. 
* ``filesystem``: A path to the directory where buildes and partitions will be stored.
* ``remotes``: A list of cache strings, referencing a remote library where bundles will be synchronized from with :command:`ambry sync`

Since the Library filesystem is where the sqlite files for bundles and partitions is stored, you may want to put it on a fast disk. 


Filesystem Section
------------------

You can change any of the paths in this section, but the most common one to change is ``root``, which will move the entire library to another directory. 

* ``root``: A substitution variable for other paths. 
* ``downloads``: Data files are downloaded to this directory
* ``extracts``: Where compressed data files are uncompressed. 
* ``documentation``: Location for generaed HTML documentation. 
* ``source``: Location for source bundles. 
* ``build``: If it exists, bundles are built here, rather than in the bundle's source directory. 
* ``python``: Install location for python packages that are referenced as dependencies in a bundle. 
* ``warehouses``: Location for storing sqlite warehouses databases. 


For light use, you won't have to change any of these values. However, for heavy use, you may want to move some of the directories to disks that are larger or faster. The directories that take up the most space are: ``build``, ``downloads`` and ``extracts``.


Account File
************

All credentials for access to restricted servers, websites and other resources are stored in the accounts file.  While there are a lot of services that can use this file, there are two sections that are most important: your name and email, for creating bundles, and S3 credentials for uploading to libraries. 

Set Your Name and Email
-----------------------

Immediately after installation, your ``~/.ambry-accounts.yaml`` file will have:

.. code-block:: yaml

    accounts:
        ambry:
            email: null
            name: null

You will need to edit your name and email to be able to create bundles. 


Set S3 Account Credentials
--------------------------

The format for each section in the account file is dependent on the account type. The most common type you will have to deal with is S3. S# account credentials are most important if you will be getting source files or bundles from a restricted repository. Here is a template for an S3 entry:

.. code-block:: yaml

    bucket.example.org:
        service: s3
        user: test
        access: AKIANOTAREALKEY45SFSF3E
        secret: EIcAj7P0MHDBv/TR63tsdgSgHjhg/g8vcK+


The key ( ``devtest.sandiegodata.org`` in this example ) is the bucket name. 


Synchronize to Remotes
**********************

After setting up configuration files, you'll need to get some data bundles. The public library has some undles you can play with, and the system library has bundles that are required for some Ambry features. To build a local library from configured remotes:

.. code-block:: bash

    $ ambry sync
    
That command may run for about 10 minutes as it downloads bundles ( but not partitions ) and installs them in the local library. After it completes, you should be able to run :command:`ambry list` to get a list of the installed files. 

Build the Index
***************

When a bundle is installed, it is automatically added to the full text search index, but the place identifiers are not. The place index  is used for converting place names, primarily US states and counties, into geoids. To build all of the indexes, including the place identifiers: 

.. code-block:: bash

    $ ambry search -R
    
When that completes, running :command:`ambry search -i california` should return results, for the places index, and :command:`ambry search proto` should return results for the dataset index. 





