.. _creating_toplevel:

=====================
Creating a New Bundle
=====================

First, you will need to properly :doc:`configure your Ambry installation <configuration>`. In particular, you will need
to set your email and name in the ``.ambry-accounts.yaml`` file.

Be sure you are running a recent version of Ambry. It's alpha software, so things break a lot. The safest bet it is to
install from git with::

    pip install -e "git+https://github.com/clarinova/ambry.git#egg=ambry"

This will install ambry in a sub directory of the current directory, ``$(pwd$/src/ambry``. You can update to the latest
Ambry by doing a ``git pull` in that directory.

In your ``Source`` directory, clone a source repo. Here are a few:

* https://github.com/sdrdl/sdrdl-ambry-bundles.git
* https://github.com/sdrdl/example-bundles.git

If you ran ``ambry config install`` as non-root::

    cd ~/ambry/source
    git clone https://github.com/sdrdl/sdrdl-ambry-bundles.git
    git clone https://github.com/sdrdl/example-bundles.git

These files can serve as examples for creating you own bundle.

You'll probably want to create your own repository, parallel to ``sdrdl-ambry-bundles`` and ``example-bundles``. In that
repository, you'll create the bundle with the ``ambry source new`` command, specifying, at least, the url of the source,
and a name of a dataset. So, if you are working with the "Employment By Industry Data", from
``http://www.labormarketinfo.edd.ca.gov``::

    cd ~/ambry/source
    ambry source new -s edd.ca.gov -d empindus

.. important::

    Every call to ``ambry source new`` calls a number server to get a unique dataset number. While the number space for
    unregistered calls is huge, it is preferable to use the local space if you will be testing the creating of bundles.
    Use the option -kself to self enerate a dataset number::

        ambry source new -s edd.ca.gov -d empindus -kself

    The self-assigned numbers are a lot longer, but they don't hit the server and don't require a network connection.

After generating the bundle source, the command will tell you where the bundle was generated. It's usually a sub-directory
of the current directory named after the source and dataset name::

    $ cd ~/ambry/source
    $ ambry source new -s edd.ca.gov -d empindus
    Installing: edd.ca.gov-empindus-0.0.1
    CREATED: edd.ca.gov-empindus-0.0.1~dKB2GX42aa001, /home/vagrant/ambry/source/edd.ca.gov/empindus
    $

Now you can change to the bundle directory and build it. When building a bundle, the command is
``ambry bundle -d <ref> build``, but that is a lot of typing, so if you are in the root directory of a bundle, you can
the ``bambry`` convenience script::

    $ cd ~/ambry/source/edd.ca.gov/empindus
    $ bambry build

When it is done, you will have a bundle database in the ``build`` subdirectory.

.. important::

    Creating a bundle source package will register the bundle with the library, so you can run ``ambry list`` to
    find the location of the bundle. But this also means that if you try to re-create the source bundle with the
    same name, you'll get a conflict. Instead, you'll have to delete the reference from the library first, using
    ```ambry library remove -s <ref>``, where ``<ref>`` is the id number of name of the bundle.

Now, you've got a new bundle, and you've verified that it can build. The next step is to update the configuration and
code.

---------------
Writing Bundles
---------------

This is a complicated topic, so you'll probably have to read a lot of other bundles while we write better documentation ...

There are a few files you will care about:

* build.py. The main bundle code
* bundle.yaml. The main configuration file.
* meta/schema.csv. A spreadsheet that specifies the schema.
* meta/build.yaml. Configuration for sources, module requirements, etc.

.. hint::

    A good bundle to study is  in ``sdrdl-ambry-bundles/edd.ca.gov/empwages``.

    Or, try finding it with ``ambry list wage`` then run ``ambry info <ref>`` for ``<ref>`` is the name or id number
    of the latest version of the bundle listed.

A few notes:

* There are a lot of bundle commands; use ``bambry -h`` to see them.
* There are multiple bundle phases. Run ``bambry prepare --clean`` to bulid the metadata database and load the schema. ``bambry build --clean`` runs the prepare phase first, then the build phase.
* The meta phase is used to automatically generate metadata, such as the schema. See the empwages buld for an example.


