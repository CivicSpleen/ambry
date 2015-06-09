.. _bundle_creating:

Creating a New Bundle
=====================


How to create a new bundle. 

.. important::

    You will need to properly :ref:`configure your Ambry installation <configuration>`. In particular, you will need to set your email and name in the :file:`$HOME/.ambry-accounts.yaml` file. 

.. note::

    This discussion will create a bundle in for the ``example.com`` source, which you can get by cloning the `example.com repository from Github <https://github.com/CivicKnowledge/example-bundles>`_
    

First, you will want to have a source repository in your source directory. To find your source directory, run :command:`ambry info`. The first directory level in the source directory is for repositories, so you'll usually want to start from cloning a repository into that directory. In that repository, you'll create the bundle with the ``ambry source new`` command, specifying, at least, the domain name of the source, and a name of a dataset. 

For this example, we will use the "Agricultural Productivity in the U.S." data from the US Department of Agriculture's Economic Research Service. The first bits of information you will need to get are the name of the dataset and the source name of the organization that published the data. You should consult the `Google Spreadsheet list of soruces. <https://docs.google.com/spreadsheets/d/1NPTHNv73Edd4QNc3jy9ektTR2P9QdxoTDz7oY7UmRJg/edit?usp=sharing>`_ and use one of the names in the ``Domain`` column, but if one does not exist, just use a sensible part of the organization's domain name. In this case, the name for the USDA ERS is `ers.usda.gov`, and our dataset will be called ``agg_product``

.. code-block:: bash

    $ cd ~/ambry/source/example-bundles # Assuming you cloned the example-bundles repo
    $ ambry source new -s ers.usda.gov -d agg_product

.. important::

    Every call to :command:`ambry source new` calls a number server to get a unique dataset number. While the 
    number space for unregistered calls is huge, it is preferable to use the local space if you will be 
    testing the creating of bundles.
    
    Use the option ``-n`` or  ``--dry-run`` to self generate a dataset number or set the ``self`` key::

        $ ambry source new -s edd.ca.gov -d empindus -kself

    The self-assigned numbers are a lot longer, but they don't hit the server and don't require a network connection.

There are many other parts of the bundle name you can set when creating a bundle, but only the source and dataset name are required. You can also set: 

* ``subset``. A minor name, for when there are many logical parts in a large dataset. 
* ``time``. An ISO date string to indicate the year or year range that distinguishes this dataset from others, as, for instance, the American COmmunity Survey
* ``space``. A spatial extend name, usually a US state abbreviation. 
* ``variation``. A variation name, most frequently "orig", for datasets that import data and make no other changes. 

Run the :command:`ambry source new -h` command to see all of the options. 

After generating the bundle source, the command will tell you where the bundle was generated. It's usually a sub-directory of the current directory named after the source and dataset name:

.. code-block:: bash

    $ cd ~/ambry/source
    $ ambry source new -s edd.ca.gov -d empindus
    Installing: ers.usda.gov-agg_product-0.0.1 
    CREATED: ers.usda.gov-agg_product-0.0.1~d0elTsAucL001, .../source/example-bundles/ers.usda.gov/agg_product
    

.. important::

    Creating a bundle source package will register the bundle with the library, so you can run ``ambry list -s`` to
    find the location of the bundle. But this also means that if you try to re-create the source bundle with the
    same name, you'll get a conflict. Instead, you'll have to delete the reference from the library first, using
    ``ambry library remove -s <ref>``, where ``<ref>`` is the id number of name of the bundle.

Selecting a Variation
*********************

There are a few special types of bundles that you can designate with the ``variation`` field. These aren't enforced; they are just conventions:

* ``index`` specifies that the bundle is an index, a complete list of the geographies or entites in a set, used to attach other bundles to
* ``cross`` specifies that the bundle is a crosswalk, which connects other datasets, usually two other indexes. 


Now, you've got a new bundle, and you've verified that it can build. The next step is to :ref:`update the configuration and code. <configure_bundle>`

