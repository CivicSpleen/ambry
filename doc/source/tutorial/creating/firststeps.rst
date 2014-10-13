.. _firststeps_toplevel:


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


