Ambry Databundles
================

Install
=======

See http://ambry.io for the general documentation, http://docs.ambry.io/en/latest/install_config/install.html for instgallation, 
and http://docs.ambry.io/en/latest/install_config/configuration.html for additional configuration. 

Setup with Miniconda on Mac
===========================

You can setup Ambry as a normal package, but the geographic library, GDAL, is really difficult to install, so your
Ambry installation won't produce geo databases. The best way to get GDAL installed is with Anaconda.

First, install miniconda, ( python 2.7 )
.. code-block:: bash

    $ wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
    $ bash miniconda.sh -b

    # Activate the anaconda environment
    $ export PATH=~/miniconda/bin:$PATH

Now you can create the environment.

.. code-block:: bash

    $ conda create -n ambry python

    # Where did conda put it?
    $ conda info -e

    # Now, activate it.
    $ source activate ambry

More about creating conda virtual environments: http://conda.pydata.org/docs/faq.html#env-creating

After setting up anmry, you can use conda to install gdal

.. code-block:: bash

    $ git clone https://github.com/<githubid>/ambry.git
    $ cd ambry
    $ pip install -r requirements.txt
    $ conda install gdal
    $ python setup.py devel

Running the ambry tests
=======================
.. code-block:: bash

    $ git clone https://github.com/<githubid>/ambry.git
    $ cd ambry
    $ pip install -r requirements/dev.txt
    $ python setup.py test

Test command options:
  --verbosity - verbosity of the tests, 1 by default.
  --failfast - if given, stop testing on first fail.

Examples:

.. code-block:: bash

    $ python setup.py test --verbosity=2 --failfast


FTS (Full text search) notes
===========================
Datasets search implemented on top of PostgreSQL requires postgresql-contrib and pg_trgm extension.

1. Install postgresql-contrib package.

.. code-block:: bash

    sudo apt-get install postgresql-contrib
   
2. Install pg_trgm extension:

.. code-block:: bash
    
    # switch to postgres user
    $ sudo su - postgres

    # create extension
    $ psql <db_name> -c 'CREATE EXTENSION pg_trgm;'

Postgres tests need pg_trgm extension. It's not possible to add it to the test db on the fly, so you
need to create template and add extension to the template to pass postgres tests. Later test database
will be created from that template. If postgres does not have such template all postgres tests will be skipped.

.. code-block:: bash

    $ psql postgres -c 'create database template0_trgm TEMPLATE template0;'
    $ psql template0_trgm -c 'CREATE EXTENSION pg_trgm;'

    # To create database from template we need copy permission.
    $ psql postgres -c "UPDATE pg_database SET datistemplate = TRUE WHERE datname='template0_trgm';"
