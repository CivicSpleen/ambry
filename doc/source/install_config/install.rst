.. _install:

################
Installing Ambry
################

Ambry is a complex package that has dependencies on a lot of other code, some of which are hard to build from source, so there are a few different was to install it. 

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

After installing Anaconda, open a new Terminal window ( an old one won't have the path set correctly. ) You should now be running the python included with anaconda:
 
.. code-block:: bash

    $ which python 
    /Users/eric/anaconda/bin/python
 
Most python dependencies are installed with `pip` along with Ambry, but `gdal` requires compiling and doesn't build easily on OS X, so we'll get it with conda:
 
.. code-block:: bash

    $ conda install gdal
 
Finally, install Ambry with pip:
    
.. code-block:: bash

    $ pip install ambry

Now see :ref:`install-post-install` to create your configuration files and check the integrity of the installation. 
    

Script Install
--------------

Although the OS X is the main development platform for Ambry, the OS X installation is a bit difficult, requiring a few outside packages. In particular, it will require:

* XCode, for the system compiler
* Homebrew, to install required binary packages
* The KyngChaos GDAL Complete package, for GDAL, Numpy and Sqlite.

The installation script  will walk you through installing all of these packages, but if the script fails, you may have install them yourself.

To run the automated installer, execute this line from a Terminal:

.. code-block:: bash

    $ sudo sh -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/master/support/install-osx.sh)"

When it detects a missing packages that you have to install manually, the script will pause and open a web browser for you. If those external installs have any hickups, you may have to run the installer script more than once. 

If the script fails, it is usually a problem with installing one of the external packages. You can try to install them outside of the sccript, then run the script to finish.

* For XCode, use the Apple App Store.
* For Homebrew, visit http://brew.sh/
* For the KyngChaos Packages, visit http://www.kyngchaos.com/software/frameworks#gdal_complete

The KyngChaos packages aren't signed, so Mac OS will issue a warning. Use the right-click menu to open them with the installer.

Then,  see :ref:`install-post-install` to create your configuration files and check the integrity of the installation. 
   
*************
Linux, Ubuntu
*************

For Ubuntu 13.04 through 14.04, use this script to install the dependencies and the Ambry package.

.. code-block:: bash

    $ sudo sh -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/master/support/install-ubuntu.sh)"

If you've created a fresh Ubuntu install, you'll probably also have to update and install curl: 

.. code-block:: bash

    $ apt-get update && apt-get install -y curl && \
    sudo sh -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/master/support/install-ubuntu.sh)"


Follow up with :ref:`install-post-install` to create your configuration files and check the integrity of the installation. 
   

*************
Windows
*************

For Windows, you can probably use the "Easy Way" installation: install Anaconda first, then Ambry. If that doesn't work, try Vagrant or Docker. 

*************
Vagrant
*************

To setup Ambry in Vagrant, `install vagrant <http://docs.vagrantup.com/v2/installation/index.html>`_, then get the source code. The Vagrant environment is inside the source distribution, and share's the host's source directory, so the Vagrant installation is a good way to develop on Windows while running in Ubuntu. 

First, clone the source from github, https://github.com/CivicKnowledge/ambry.git or, download a Zip archive from: https://github.com/CivicKnowledge/ambry/archive/master.zip

After unpacking the source, change directory to the vagrant directory, :file:`support/ambry-vagrant` and run :command:`vagrant up`

.. code-block:: bash

    $ wget https://github.com/CivicKnowledge/ambry/archive/master.zip
    $ unzip master.zip
    $ cd ambry-master/support/ambry-vagrant/
    $ vagrant up
    
When the build is done, ssh to the box. 

.. code-block:: bash

    $ vagrant ssh 

Then, follow the instrictions at :ref:`install-post-install` to create your configuration files and check the integrity of the installation. 
  

*************
Docker
*************
   
A Dockerfile for a basic docker image is available in: :file:`support/ambry-docker`. To build it, run:

.. code-block:: bash

    $ docker build -t ambry .

When that is finished, you can run the image with:

.. code-block:: bash

    $ docker run -i -t ambry bin/bash

.. _install-post-install:

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








