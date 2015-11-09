Test Bundles
============

These test bundles


Creating new Test Bundles.
--------------------------

To create a new test bundle, `cd` to the `test/test_bundles` directory and run:

    $ bambry new -s <test_section>.example.com -d <test_name>
    $ bambry export -d -a .

Where <test_section> is the top level of one of the test source domain names:

    * ingest.example.com
    * build.example.com
    * metadata.example.com


Configuration
-------------

In the bundle.yaml file, be sure to set the about.title and about.summary and set the about.remote value to 'test'.

