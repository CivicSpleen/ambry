
.. _mass_operations:

Write programs to manipulate a lot of bundles at once

Example of setting a metadata variable in a source subdirectory 

.. code-block:: yaml

    import ambry
    from ambry.identity import LocationRef

    l = ambry.library()

    #print l.source.list()

    for  vid, e  in l.list().items():    
        if e.locations.has(LocationRef.LOCATION.SOURCE):
            try:
                b = l.source.resolve_bundle(e.vid)
                if b.path.startswith('/data/source/sdrdl-ambry-bundles/'):
                    b.metadata.about.access = 'sdrdl'
                    b.metadata.write_to_dir()
            except ImportError:
                pass