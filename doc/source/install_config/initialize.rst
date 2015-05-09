.. _initialize:

After installing and configuring, you'll need to synchronize with a library to get databundles. At the very least, you'll need the ``civicknowledge.com-terms`` bundle to build the search index. 

Install Configuration
*********************



If you haven't already done so, install the configuration file, run:

.. code-block:: bash

    $ ambry config install

See :ref:`configuration_generation` for more information about the configuration file installation. 


Synchronize to Remotes
**********************

To build a local library from configured remotes:

.. code-block:: bash

    $ ambry sync
    
That command may run for about 10 minutes as it downloads bundles ( but not partitions ) and installs them in the local library. After it completes, you should be able to run :command:`ambry list` to get a list of the installed files. 

Build the Index
***************

When a bundle is installed, it is automatically added to the full text search index, but the place identifiers are not. The place index  is used for converting place names, primarily US states and counties, into geoids. To build all of the indexes, including the place identifiers: 

.. code-block:: bash

    $ ambry search -R
    
When that completes, running :command:`ambry search -i california` should return results. 
