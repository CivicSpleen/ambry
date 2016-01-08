.. _install:

################
Installing Ambry
################

Ambry is a complex package that has dependencies on a lot of other code, some of which are hard to build from source, so there are a few different ways to install it. 

* `Mac OS X`_
* `Linux, Ubuntu`_
* `Windows`_
* `Vagrant`_
* `Docker`_


********
Mac OS X
********


Easy Way
--------

The easiest way to install ambry is to for install the scientific python distribution, Anaconda. Visit the `Continuum Analytics downloads page <http://continuum.io/downloads>`_ and get the Anaconda installation for your distribution.

You'll probably also need to install XCode, since that's required for just about anything interesting. 

After installing Anaconda and XCode, open a new Terminal window ( an old one won't have the path set correctly. ) You should now be running the python included with anaconda:
 
Install the psycopg package, since building it is a bit hard to get right via a simple pip install. 

.. code-block:: bash

    $ conda install psycopg2


Finally, install Ambry with pip:
    
.. code-block:: bash

    $ pip install ambry

Now see :ref:`install-post-install` to create your configuration files and check the integrity of the installation. 
    

   
*************
Linux, Ubuntu
*************

If you've created a fresh Ubuntu install, you'll probably also have to update and install ``pip``. Additionally, the
``psycopg2`` package for postgres requires extra dependencies, so it beter to install it with apt-get

.. code-block:: bash

    $ apt-get update && apt-get -y  install python-pip python-psycopg2 

After that, install Ambry:

.. code-block:: bash

    $ pip install ambry


Follow up with :ref:`install-post-install` to create your configuration files and check the integrity of the installation. 
   
*************
Windows
*************

For Windows, use Docker, or an Ubuntu VM.


*************
Docker
*************

Ambry is also distributed as docker images, so you can start Ambry from any docker installation with:

.. code-block:: bash

    $ docker run -t -i civicknowledge/ambry /bin/bash 
   
:ref:`tutorial/docker`

*************
Virtual Env
*************


.. code-block:: bash

    $ mkvirtualenv client
    $ cdvirtualenv


For a development environment, or bleeding-edge use:

.. code-block:: bash

    $ git clone https://github.com/CivicKnowledge/ambry.git
    $ ambry config install



*************
Post Install
*************
  
After installing Ambry and its dependencies, you can check that the installation worked correctly with:
   
.. code-block:: bash
    
    $ ambry info 
    Version:  0.3.420
    Root dir: /home/eric/ambry
    Source :  /home/eric/ambry/source
    Configs:  ['/home/eric/.ambry.yaml', '/home/eric/.ambry-accounts.yaml']

    $ ambry library info 
    Library Info
    Name:     default
    Database: sqlite:////home/eric/ambry/library.db
    Cache:    FsCache: dir=/home/eric/ambry/library upstream=(None)
    Remotes:  FsCompressionCache: upstream=(HttpCache: url=http://s3.sandiegodata.org/library/)

After installation, you can customize the configuation. See: :ref:`configuration`








