.. _bundle_creating:

Creating a New Bundle
=====================


How to create a new bundle. 

.. important::

    First, you will need to properly :ref:`configure your Ambry installation <configuration>`. In particular, you will need to set your email and name in the ``.ambry-accounts.yaml`` file.


First, you will want to have a source repository in your source directory. To find your source directory, run :command:`ambry info`. The first directory level in the source directory is for repositories, so you'll usually want to start from cloning a repository into that directory. In that
repository, you'll create the bundle with the ``ambry source new`` command, specifying, at least, the url of the source,
and a name of a dataset. So, if you are working with the "Employment By Industry Data", from
``http://www.labormarketinfo.edd.ca.gov``:

.. code-block:: bash

    cd ~/ambry/source/<your-git-repo>
    ambry source new -s edd.ca.gov -d empindus

The source name, the option to the ``-s`` option, is usually part of the domain name for the source. We're developing a standard list of names for these sources, so these are controlled, well-known words. We're still developing the complete list, but there is an `interim list in a Google Spreadsheet. <https://docs.google.com/spreadsheets/d/1NPTHNv73Edd4QNc3jy9ektTR2P9QdxoTDz7oY7UmRJg/edit?usp=sharing>`_

There are many other parts of the bundle name you can set when creating a bundle, but only the source and majo name are required. You can also set: 

* ``subset``. A minor name, for when there are many logical parts in a large dataset. 
* ``time``. An ISO date string to indicate the year or year range that distinguishes this dataset from others, as, for instance, the American COmmunity Survey
* ``space``. A spatial extend name, usually a US state abbreviation. 
* ``variation``. A variation name, most frequently "orig", for datasets that import data and make no other changes. 

Run the :command:`ambry source new -h` command to see all of the options. 

.. important::

    Every call to ``ambry source new`` calls a number server to get a unique dataset number. While the number space for
    unregistered calls is huge, it is preferable to use the local space if you will be testing the creating of bundles.
    Use the option -n or  --dry-run to self generate a dataset number or set the ``self`` key::

        ambry source new -s edd.ca.gov -d empindus -kself

    The self-assigned numbers are a lot longer, but they don't hit the server and don't require a network connection.

After generating the bundle source, the command will tell you where the bundle was generated. It's usually a sub-directory of the current directory named after the source and dataset name:

.. code-block:: bash

    $ cd ~/ambry/source
    $ ambry source new -s edd.ca.gov -d empindus
    Installing: edd.ca.gov-empindus-0.0.1
    CREATED: edd.ca.gov-empindus-0.0.1~dKB2GX42aa001, /home/vagrant/ambry/source/edd.ca.gov/empindus
    $

Now you can change to the bundle directory and build it. When building a bundle, the command is
``ambry bundle -d <ref> build``, but that is a lot of typing, so if you are in the root directory of a bundle, you can
the ``bambry`` convenience script:

.. code-block:: bash

    $ cd ~/ambry/source/edd.ca.gov/empindus
    $ bambry build

When it is done, you will have a bundle database in the ``build`` subdirectory.

.. important::

    Creating a bundle source package will register the bundle with the library, so you can run ``ambry list`` to
    find the location of the bundle. But this also means that if you try to re-create the source bundle with the
    same name, you'll get a conflict. Instead, you'll have to delete the reference from the library first, using
    ``ambry library remove -s <ref>``, where ``<ref>`` is the id number of name of the bundle.

Selecting a Variation
*********************

There are a few special types of bundles that you can designate with the ``variation`` field:

* ``index`` specifies that the bundle is an index, a complete list of the geographies or entites in a set, used to attach other bundles to
* ``cross`` specifies that the bundle is a crosswalk, which connects other datasets, usually two other indexes. 


Now, you've got a new bundle, and you've verified that it can build. The next step is to :ref:`update the configuration and code. <configure_bundle>`

