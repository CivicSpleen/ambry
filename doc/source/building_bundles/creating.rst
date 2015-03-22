.. _skeleton:

=====================
Creating a New Bundle
=====================

.. important::

    First, you will need to properly :ref:`configure your Ambry installation <configuration>`. In particular, you will need to set your email and name in the ``.ambry-accounts.yaml`` file.


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

    cd ~/ambry/source/<your-git-repo>
    ambry source new -s edd.ca.gov -d empindus

.. important::

    Every call to ``ambry source new`` calls a number server to get a unique dataset number. While the number space for
    unregistered calls is huge, it is preferable to use the local space if you will be testing the creating of bundles.
    Use the option -n or  --dry-run to self generate a dataset number::

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

