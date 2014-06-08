.. _creating_toplevel:

=====================
Creating a New Bundle
=====================

First, you will need to properly :doc:`configure your Ambry installation <configuration>`. In particular, you will need
to set your email and name in the ``.ambry-accounts.yaml`` file.

Be sure you are running a recent version of Ambry. It's alpha software, so things break a lot. The safest bet it is to
install from git with::

    pip install -e "git+https://github.com/clarinova/ambry.git#egg=ambry"

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

    Every call to ``ambry source new` calls a number server to get a unique dataset number. While the number space for
    unregistered calls is huge, it is preferable to use the local space if you will be testing the creating of bundles.
    There currently isn't a way to s
