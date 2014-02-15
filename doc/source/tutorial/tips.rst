.. _tips_toplevel:

===============
Tips and Tricks
===============

Downloading Files
-----------------

Use :func:`ambry.filesystem.download` to download URLS. 

If the configuration has a *build.sources* value, you can define the url in the configuartion and use the key for the entry as the URL. 

You expect the URL to be a zip file, use :func:`ambry.filesystem.unzip` to extract files from the zipfile. 

Storing Meta Data
-----------------

The Meta phase is useful for creating information that needs only be created once, like schemas. Some information is best stored in .yaml files, which can be created and read with:

  - :func:`ambry.filesystem.write_yaml`
  - :func:`ambry.filesystem.read_yaml`


