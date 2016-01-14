.. _install:


Installing Ambry
################

Ambry is a complex package that has dependencies on a lot of other code, some of which are hard to build from source, so there are a few different ways to install it. 

* `Mac OS X`_
* `Linux, Ubuntu`_
* `Windows`_
* `Vagrant`_
* `Docker`_


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
   

Linux, Ubuntu
*************

For Ubuntu 13.04 through 14.04, use this script to install the dependencies and the Ambry package.

.. code-block:: bash

    $ sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/master/support/install-ubuntu.sh)"

If you've created a fresh Ubuntu install, you'll probably also have to update and install curl. Here's one line that will take care of everything.  

.. code-block:: bash

    $ apt-get update && apt-get install -y curl && \
    sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/CivicKnowledge/ambry/master/support/install-ubuntu.sh)"


Follow up with :ref:`install-post-install` to create your configuration files and check the integrity of the installation. 
   

*************
Windows
*************

For Windows, you can probably use the "Easy Way" installation: install Anaconda first, then Ambry. If that doesn't work, try Vagrant or Docker. 


Docker
*************
   
TBD. 


Post Install
*************
  
After installing Ambry and its dependencies, you can check that the installation worked correctly with:
   
.. code-block:: bash
    
    $ ambry info 
	Version:   0.3.1612
	Root dir:  /Users/eric/proj/virt/ambry-develop/data
	Source :   /Users/eric/proj/virt/ambry-develop/data/source
	Configs:   ['/Users/eric/proj/virt/ambry-develop/.ambry.yaml']
	Accounts:  /Users/eric/.ambry-accounts.yaml
	Library:   sqlite:////Users/eric/proj/virt/ambry-develop/data/library.db
	Remotes:   test, public

After installation, you can customize the configuation. See: :ref:`configuration`


