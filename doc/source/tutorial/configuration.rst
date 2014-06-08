.. _configuration:

=============
Configuration
=============

Now to configure your Ambry installation.

First, run `ambry config` to find the location of your configuration files and your source directory.

Edit the `.ambry-account.yaml` file to set your email and name

You may want to get some example bundle repositories, if you will be building bundles. You can run `ambry info` to
find your source directory, then, in that directory, clone::

	https://github.com/sdrdl/sdrdl-ambry-bundles.git

	https://github.com/sdrdl/example-bundles.git

Run `ambry library sync` to synchronize your library with the remote and sources.

Run `ambry list` to ensure you get a list of bundles, some of which should be source, if you cloned source.


