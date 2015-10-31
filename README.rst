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

Postgres extensions notes (Note: If you use virtualenv see DEVEL-README.md)
==========================================================================
Full text search

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

Foreign Data Wrapper (need to query partition files packed with psgpack.)

1. Install multicorn:
.. code-block:: bash

    wget https://github.com/Kozea/Multicorn/archive/v1.2.3.zip
    unzip v1.2.3.zip
    cd Multicorn-1.2.3
    make && sudo make install

2. Install ambryfdw:
.. code-block:: bash
    pip install ambryfdw

CKAN export
===========
1. Add CKAN credentials to ~/.ambry-accounts.yaml:

.. code-block:: yaml
ckan:
    host: http://demo.ckan.org
    organization: org1
    apikey: <your API key>

2. Run

.. code-block:: bash
ambry ckan_export <dataset_vid>
